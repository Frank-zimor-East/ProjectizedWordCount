package Streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util
import java.sql.Connection


case class UserClickAction( timestamp: Long, city: String, area: String, user: Int, ad: Int )

object UserClickAd {
  def main( args: Array[String] ): Unit = {


    //1.TODO 配置环境,读取数据源 Kafka
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("BlackListUser")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    conf.registerKryoClasses(util.Arrays.asList(classOf[ConsumerRecord[_, _]]).toArray.asInstanceOf[Array[Class[_]]])
    val sc: StreamingContext = StreamingContext.getActiveOrCreate(( ) => new StreamingContext(conf, Seconds(5)))

    //    sc.checkpoint("/resources/checkpoint")
    val kafkaTopic = "user_click_ad"
    val stream: InputDStream[ConsumerRecord[String, String]] = GetConnections.getKafkaStream(sc, kafkaTopic)
    val userCountTable = "spark.user_ad_count"
    //    stream.print(5)

    val userActions: DStream[UserClickAction] = stream.map(record => {
      val fields: Array[String] = record.value().split(",")
      val timestamp: Long = fields(0).toLong
      val city: String = fields(1)
      val area: String = fields(2)
      val user: Int = fields(3).toInt
      val ad: Int = fields(4).toInt
      UserClickAction(timestamp, city, area, user, ad)
    })

    //    userActions.print(5)
    //2.TODO 读取当前数据,查看是否在Mysql黑名单中
    userActions
      .window(Seconds(10))
      .map(user => ((user.user, user.ad), 1))
      .reduceByKey(_ + _)

      .foreachRDD(rdd => {
        rdd.foreachPartition {
          iters => {
            val connection: Connection = GetConnections.initConnection()
            iters.foreach {
              case ((user, ad), count) => {
                //判断是否在黑名单
                val inBlackList: Boolean = GetConnections.readBlackListTable(connection, userCountTable, user, ad)
                if (!inBlackList) {
                  val original_cnt: Int = GetConnections.readUserCountTable(connection, userCountTable, user, ad)
                  GetConnections.updateUserCountTableIgnoreExists(connection, userCountTable, user, ad, count + original_cnt)
                }

              }

            }
            connection.close()
          }
        }
        rdd
      })



    //3.TODO 不在放行,计数,更新用户点击数量表 ; 在 直接过滤掉不做任何计算

    sc.start()
    sc.awaitTermination()
  }
}
