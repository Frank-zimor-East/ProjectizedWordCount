import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


import java.{lang, util}
import java.util.Properties


object Test extends App {





  var kafkaProps: Map[String, String] = Map[String, String]()
  kafkaProps += ("bootstrap.servers" -> "192.168.10.100:9092,192.168.10.101:9092,192.168.10.102:9092")
  kafkaProps += ("group.id" -> "blackList1")
  kafkaProps += ("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps += ("value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps += ("enable.auto.commit" ->"true")



  private val prop = new Properties()
  kafkaProps.foreach(
    tup => prop.put(tup._1, tup._2)
  )


  private val consumer = new KafkaConsumer[String, String](prop,new StringDeserializer,new StringDeserializer)
  consumer.subscribe(util.Arrays.asList("user_click_ad"))


  var i = 0
  while (true) {
    try {
      val records: ConsumerRecords[String, String] = consumer.poll(1000)
      import scala.collection.JavaConversions._
      for (record <- records) {
        //统计各个地区的客户数量，即模拟对消息的处理
        println(record.offset())
        println(record.key())
        println(record.value())
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      i += 1
      println(i)
    }
  }

}
