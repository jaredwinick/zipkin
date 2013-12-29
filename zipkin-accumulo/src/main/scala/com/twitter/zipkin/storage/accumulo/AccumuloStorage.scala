package com.twitter.zipkin.storage.accumulo

import com.twitter.zipkin.storage.Storage
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.data.Range
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.gen
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.zipkin.util.AccumuloThreads.inNewThread
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import com.twitter.scrooge.BinaryThriftStructSerializer
import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.security.Authorizations
import scala.collection.JavaConversions._
import java.util.Map.Entry
import java.util.concurrent.TimeUnit
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.iterators.user.WholeRowIterator

case class AccumuloStorage(
    instanceName: String, 
    zooKeepers: String,
    user: String,
    password: String) extends Storage {
  
  val BATCH_WRITER_MAX_LATENCY_MILLISECONDS = 1000

  val instance: Instance = new ZooKeeperInstance(instanceName, zooKeepers)
  val connector: Connector = instance.getConnector(user, new PasswordToken(password))
  
  //TODO: assuming table exists
  val table = "zipkin_traces"
    
  //TODO: allow more configuration to be passed in
  val authorizations = new Authorizations()
  val batchWriterConfig = new BatchWriterConfig()
  batchWriterConfig.setMaxLatency(BATCH_WRITER_MAX_LATENCY_MILLISECONDS, TimeUnit.MILLISECONDS)
  val batchWriter: BatchWriter = connector.createBatchWriter(table, batchWriterConfig)
  
  // used to serialize Span to bytes
   val serializer = new BinaryThriftStructSerializer[gen.Span] {
    def codec = gen.Span
  }
  
  def close() = {  
    // Until ACCUMULO-1379 is in 1.5.x or we switch to 1.6, we can't close the instance
    
    batchWriter.close()
  }
  
  /**
   * Store the span in the underlying storage for later retrieval.
   * NOTE: writes are not immediately flushed by the BatchWriter
   * If this behavior is not inline with the intent of this method, then 
   * we can call flush() on the BatchWriter for each Span
   * @return a future for the operation
   */
  def storeSpan(span: Span) : Future[Unit] = inNewThread {
    val spanMutation = new Mutation(traceIdToRowId(span.traceId))
    val cqString = span.id.toString + span.annotations.hashCode.toString
    spanMutation.put(new Text("span"), new Text(cqString), new Value(serializer.toBytes(span.toThrift)))
    batchWriter.addMutation(spanMutation)
  }

  /**
   * Set the ttl of a trace. Used to store a particular trace longer than the
   * default. It must be oh so interesting!
   */
  def setTimeToLive(traceId: Long, ttl: Duration): Future[Unit] = {
    Future.Unit
  }

  /**
   * Get the time to live for a specific trace.
   * If there are multiple ttl entries for one trace, pick the lowest one.
   */
  def getTimeToLive(traceId: Long): Future[Duration] = {
    Future.value(Duration.Top)
  }

  /**
   * Finds traces that have been stored from a list of trace IDs
   *
   * @param traceIds a List of trace IDs
   * @return a Set of those trace IDs from the list which are stored
   */
  def tracesExist(traceIds: Seq[Long]): Future[Set[Long]] = inNewThread {
    // convert each traceId to a Range and use a BatchScanner to find entries
    val ranges = traceIds.map(traceIdToRange)
    asScalaIterable(createTracesExistBatchScannerWithRanges(ranges)).map { entry =>
      entryToTraceId(entry)
    }.toSet
  }
  
  private[this] def createTracesExistBatchScannerWithRanges(ranges: Seq[Range]): BatchScanner = {
    val batchScanner = createBatchScanner()
    
    // we only want the first entry in each row as that is all that is needed to tell if a trace exists
    val firstEntryInRowIteratorSetting = new IteratorSetting(100, classOf[FirstEntryInRowIterator])
    batchScanner.addScanIterator(firstEntryInRowIteratorSetting)

    // set the ranges - one for each traceId we are looking up
    batchScanner.setRanges(asJavaList(ranges))
    
    return batchScanner
  }
  
  private[this] def createBatchScanner(): BatchScanner = {
    connector.createBatchScanner(table, authorizations, 10)
  }
  
  private[this] def createGetSpansBatchScanner(ranges: Seq[Range]): BatchScanner = {
    val batchScanner = createBatchScanner()
    
    // we want all Spans for a Trace (all Values with the same RowId) to be returned at once
    val wholeRowIteratorSetting = new IteratorSetting(100, classOf[WholeRowIterator])
    batchScanner.addScanIterator(wholeRowIteratorSetting)

    batchScanner.setRanges(asJavaList(ranges))
    
    return batchScanner
  }
  
  private[this] def entryToTraceId(entry: Entry[Key,Value]): Long = {
  	rowIdToTraceId(entry.getKey().getRow())
  }
  
  private[this] def traceIdToRowId(traceId: Long): Text = {
    new Text(java.lang.Long.toHexString(traceId))
  }
  
  private[this] def rowIdToTraceId(rowId: Text): Long = {
    java.lang.Long.parseLong(rowId.toString(), 16)
  }
  
  private[this] def traceIdToRange(traceId: Long): Range = {
    new Range(traceIdToRowId(traceId))
  }
  

  /**
   * Get the available trace information from the storage system.
   * Spans in trace should be sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   */
  def getSpansByTraceIds(traceIds: Seq[Long]): Future[Seq[Seq[Span]]] =  inNewThread {
    val ranges = traceIds.map(traceIdToRange)
   
    // each Entry is a whole row containing all the Spans as Values.
    // we will need to sort the Spans in a Trace, and then also sort the 
    // list of Spans that are returned
    asScalaIterable(createGetSpansBatchScanner(ranges)).map { entry =>
      wholeRowToSpans(entry).sortBy { span =>
        getTimestamp(span) 
      } 
    }.toList.sortBy { spans =>
      getTimestamp(spans.head)
    }
  }
    
  // via com.twitter.zipkin.storage.hbase
  private[this] def getTimestamp(span: Span): Option[Long] = {
    val timeStamps = span.annotations.map {_.timestamp}.sortWith(_ < _)
    timeStamps.headOption
  }
  
  private[this] def wholeRowToSpans(wholeRowEntry: Entry[Key,Value]): Seq[Span] = {

    asScalaIterable(WholeRowIterator.decodeRow(wholeRowEntry.getKey(), wholeRowEntry.getValue()).values()).map { value =>
      serializer.fromBytes(value.get()).toSpan
    }.toList

  }
  
  def getSpansByTraceId(traceId: Long): Future[Seq[Span]] = {
    getSpansByTraceIds(Seq(traceId)).map {
      _.head
    }
  }
  
  /**
   * How long do we store the data before we delete it? In seconds.
   */
  def getDataTimeToLive: Int = {
    Int.MaxValue
  }
}