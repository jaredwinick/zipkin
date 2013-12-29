package com.twitter.zipkin.storage.accumulo

import org.specs.Specification
import com.twitter.util.Await
import com.google.common.io.Files
import java.io.File
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Value, Key, Mutation}
import org.apache.accumulo.core.security.{ColumnVisibility, Authorizations}
import java.util.Map.Entry
import com.twitter.zipkin.common.Annotation
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.common.Endpoint
import com.twitter.zipkin.common.BinaryAnnotation
import java.nio.ByteBuffer
import com.twitter.zipkin.common.AnnotationType
import com.twitter.zipkin.query.Trace

class AccumuloStorageSpec extends Specification {

  // Much of this borrowed directly from AnormStorageSpec 
  def binaryAnnotation(key: String, value: String) =
    BinaryAnnotation(key, ByteBuffer.wrap(value.getBytes), AnnotationType.String, Some(ep))
    
  val ep = Endpoint(123, 123, "service")

  val spanId = 456
  val traceIdDNE = 456
  val ann1 = Annotation(1, "cs", Some(ep))
  val ann2 = Annotation(2, "sr", None)
  val ann3 = Annotation(2, "custom", Some(ep))

  val span1 = Span(123, "methodcall", spanId, None, List(ann1, ann3),
    List(binaryAnnotation("BAH", "BEH")))
  val span2 = Span(667, "methodcall2", spanId, None, List(ann2),
    List(binaryAnnotation("BAH2", "BEH2")))
    
  var miniAccumuloCluster: MiniAccumuloCluster = null
  var connector: Connector = null
  var instance: Instance = null
    
  "AccumuloStorage" should {  

    doBefore {
      val tempDirectory = Files.createTempDir()
      miniAccumuloCluster = new MiniAccumuloCluster(tempDirectory, "password")
      miniAccumuloCluster.start()
      println("ZK:" + miniAccumuloCluster.getZooKeepers())
      
      // This shouldn't be here - we should make the AccumuloStorage handled creating tables
      instance = new ZooKeeperInstance(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers())
      connector = instance.getConnector("root", new PasswordToken("password"))
      connector.tableOperations().create("zipkin_traces")
    }
    
    doAfter {
      miniAccumuloCluster.stop()
      miniAccumuloCluster = null
    }

 
    "tracesExist" in {

      val storage = new AccumuloStorage(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers(), "root", "password")
      Await.result(storage.storeSpan(span1))
      Await.result(storage.storeSpan(span2))

      // Wait at least the batchWriter latency to make sure writes are flushed
      Thread.sleep(storage.BATCH_WRITER_MAX_LATENCY_MILLISECONDS + 1000)
      
      Await.result(storage.tracesExist(List(span1.traceId, span2.traceId, traceIdDNE))) must haveTheSameElementsAs(Set(span1.traceId, span2.traceId))
      Await.result(storage.tracesExist(List(span2.traceId))) must haveTheSameElementsAs(Set(span2.traceId))
      Await.result(storage.tracesExist(List(traceIdDNE))).isEmpty mustEqual true

      storage.close()
    }
  
    
    "getSpansByTraceId" in {
      val storage = new AccumuloStorage(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers(), "root", "password")

      Await.result(storage.storeSpan(span1))
      Await.result(storage.storeSpan(span2))
      
       // Wait at least the batchWriter latency to make sure writes are flushed
      Thread.sleep(storage.BATCH_WRITER_MAX_LATENCY_MILLISECONDS + 1000)

      val spans = Await.result(storage.getSpansByTraceId(span1.traceId))
      spans.isEmpty mustEqual false
      spans(0) mustEqual span1
      spans.size mustEqual 1

      storage.close()
    }
    
    "getSpansByTraceIds" in {
    	val storage = new AccumuloStorage(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers(), "root", "password")

      Await.result(storage.storeSpan(span1))
      Await.result(storage.storeSpan(span2))
      
       // Wait at least the batchWriter latency to make sure writes are flushed
      Thread.sleep(storage.BATCH_WRITER_MAX_LATENCY_MILLISECONDS + 1000)

      val emptySpans = Await.result(storage.getSpansByTraceIds(List(traceIdDNE)))
      emptySpans.isEmpty mustEqual true

      val oneSpan = Await.result(storage.getSpansByTraceIds(List(span1.traceId)))
      oneSpan.isEmpty mustEqual false
      val trace1 = Trace(oneSpan(0))
      trace1.spans.isEmpty mustEqual false
      trace1.spans(0) mustEqual span1

      
      val twoSpans = Await.result(storage.getSpansByTraceIds(List(span1.traceId, span2.traceId)))
      twoSpans.isEmpty mustEqual false
      val trace2a = Trace(twoSpans(0))
      val trace2b = Trace(twoSpans(1))
      trace2a.spans.isEmpty mustEqual false
      trace2a.spans(0) mustEqual span1
      trace2b.spans.isEmpty mustEqual false
      trace2b.spans(0) mustEqual span2
      
      storage.close()
    }

  }

}
