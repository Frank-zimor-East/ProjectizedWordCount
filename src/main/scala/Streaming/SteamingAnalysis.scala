package Streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random


object SteamingAnalysis {

  def main( args: Array[String] ): Unit = {

    def createKafkaProducer( ) :KafkaProducer[String, String]= {
      val props: Properties = new Properties()
      props.put("bootstrap.servers", "192.168.10.100:9092,192.168.10.101:9092,192.168.10.102:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      producer
    }

    val rand: Random = new Random()
    def generateProduceRecord(rand:Random): ProducerRecord[String, String] ={
      val topic: String = "user_click_ad"
      val cityList = List(("北京", "华北"), ("上海", "华东"), ("广州", "华南"), ("深圳", "华南"), ("天津", "华北"))
      val timestamp: Long = rand.nextInt(864500000 * 2) + 1622476800000L
      val city_area: (String, String) = cityList(rand.nextInt(cityList.size))
      val city: String = city_area._1
      val area: String = city_area._2
      val user: Int = rand.nextInt(5)
      val ad: Int = rand.nextInt(5)
      val record= new ProducerRecord[String,String](topic, timestamp + "," + city + "," + area + "," + user + "," + ad)
      record
    }

    val producer: KafkaProducer[String, String] = createKafkaProducer()
    for (i <- 0 to 2000) {
      // 6.1 接下来两天随机
      producer.send(generateProduceRecord(rand))
      Thread.sleep(300)
//      print(1)

    }
    print(1)
    /**
     * 格式 ：timestamp area city userid adid
     * 某个时间点 某个地区 某个城市 某个用户 某个广告
     * RanOpt(CityInfo(1, "北京", "华北"), 30),
     * RanOpt(CityInfo(2, "上海", "华东"), 30),
     * RanOpt(CityInfo(3, "广州", "华南"), 10),
     * RanOpt(CityInfo(4, "深圳", "华南"), 20),
     * RanOpt(CityInfo(5, "天津", "华北"), 10))
     */

    //循环产生数据写入kafka


  }
}
