package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object StreamingTest extends App {

  private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming")
  private val streamingContext = new StreamingContext(conf, Seconds(5))

  var stop_or_not: Boolean = false


  private val stream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)

  private val cnt: DStream[Long] = stream.map(line => {
    val fields: Array[String] = line.split(" ")
    User(fields(0), fields(1))
  }).count()

  cnt.print()


  streamingContext.start()
  streamingContext.awaitTermination()

//  streamingContext.stop()

}

case class User( name: String, age: String )
