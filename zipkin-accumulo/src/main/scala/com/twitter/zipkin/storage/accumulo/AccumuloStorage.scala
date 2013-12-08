package com.twitter.zipkin.storage.accumulo

import com.twitter.zipkin.storage.Storage
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import com.twitter.zipkin.common.Span
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.zipkin.util.AccumuloThreads.inNewThread
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import scala.runtime.RichLong
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.data.Value

case class AccumuloStorage(
    instanceName: String, 
    zooKeepers: String,
    user: String,
    password: String) extends Storage {

  val instance: Instance = new ZooKeeperInstance(instanceName, zooKeepers)
  val connector: Connector = instance.getConnector(user, new PasswordToken(password))
  
  //TODO: assuming table exists
  val table = "zipkin_traces"
  val batchWriter: BatchWriter = connector.createBatchWriter(table, new BatchWriterConfig())

  def close() = {  
    // Nothing to do to close until ACCUMULO-1379 is in 1.5.x or we switch to 1.6
  }
  
  /**
   * Store the span in the underlying storage for later retrieval.
   * @return a future for the operation
   */
  def storeSpan(span: Span) : Future[Unit] = inNewThread {
    println("Store span")
    
    val spanMutation = new Mutation(new Text(java.lang.Long.toHexString(span.traceId)))
    val parentIdString = java.lang.Long.toHexString(span.parentId.getOrElse(0)) 
    val cqString = parentIdString + ":" + java.lang.Long.toHexString(span.id)
    spanMutation.put(new Text("span"), new Text(cqString), new Value(Array[Byte]()))
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

  def tracesExist(traceIds: Seq[Long]): Future[Set[Long]] = inNewThread {
    Set(0)
  }

  /**
   * Get the available trace information from the storage system.
   * Spans in trace should be sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   */
  def getSpansByTraceIds(traceIds: Seq[Long]): Future[Seq[Seq[Span]]] =  inNewThread {
    Seq(Seq())
  }
  
  def getSpansByTraceId(traceId: Long): Future[Seq[Span]] = inNewThread {
    Seq()
  }
  
  /**
   * How long do we store the data before we delete it? In seconds.
   */
  def getDataTimeToLive: Int = {
    Int.MaxValue
  }
}