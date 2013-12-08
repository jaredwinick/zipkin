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
    
  "AccumuloStorage" should {

    var miniAccumuloCluster: MiniAccumuloCluster = null

    doBefore {
      println("Hello")
      val tempDirectory = Files.createTempDir()
      miniAccumuloCluster = new MiniAccumuloCluster(tempDirectory, "password")
      miniAccumuloCluster.start()
      println("ZK:" + miniAccumuloCluster.getZooKeepers())
      
      // This shouldn't be here - we should make the AccumuloStorage handled creating tables
      val instance: Instance = new ZooKeeperInstance(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers())
      val connector: Connector = instance.getConnector("root", new PasswordToken("password"))
      connector.tableOperations().create("zipkin_traces")
    }

    doAfter {
      miniAccumuloCluster.stop()
      println("Goodbye")
    }

    "tracesExist" in {

      val storage = new AccumuloStorage(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers(), "root", "password")
      Await.result(storage.storeSpan(span1))
      
      /*
      val instance: Instance = new ZooKeeperInstance(miniAccumuloCluster.getInstanceName(), miniAccumuloCluster.getZooKeepers())
      val connector: Connector = instance.getConnector("root", new PasswordToken("password"))
      connector.tableOperations().create("test")
      val batchWriter: BatchWriter = connector.createBatchWriter("test", new BatchWriterConfig())
      val mutation: Mutation = new Mutation("row2")
      mutation.put("cf","cq",new Value("1".getBytes()))
      batchWriter.addMutation(mutation)
      batchWriter.flush()

      val scanner = connector.createScanner("test", new Authorizations())
      val iterator = scanner.iterator()
      while (iterator.hasNext()) {

        val entry: Entry[Key, Value] = iterator.next()
        println("Key:" + entry.getKey().toString())
        println("Value:" + entry.getValue().toString())
      }
*/
      true mustEqual true
    }
  }

}
