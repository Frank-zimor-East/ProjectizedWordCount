package Streaming

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

object GetConnections {

  def initConnection( ): Connection = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://localhost:3306")
    properties.setProperty("username", "root")
    properties.setProperty("password", "000000")
    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    dataSource.getConnection
  }



  def readUserCountTable( connection:Connection,table: String, user: Int, ad: Int ): Int = {
    val statement: PreparedStatement = connection.prepareStatement(
      s"""
         |select count
         |from ${table}
         |where user =?
         |and ad =?
         |""".stripMargin)
    statement.setInt(1, user)
    statement.setInt(2, ad)

    var count = 0
    val set: ResultSet = statement.executeQuery()
    if (set.next()) {
      count = set.getInt(1)
    }
    statement.close()
    set.close()
    count
  }

  def readBlackListTable( connection:Connection,table: String, user: Int, ad: Int ): Boolean = {
    val statement: PreparedStatement = connection.prepareStatement(
      s"""
         |select count
         |from ${table}
         |where user =?
         |and ad =?
         |and count >= 20
         |""".stripMargin)
    statement.setInt(1, user)
    statement.setInt(2, ad)

    val set: ResultSet = statement.executeQuery()
    val bool: Boolean = set.next()
    statement.close()
    set.close()
    bool
  }

  //  update user_click_count set count = 100 where user = 1 and ad =1;
//  def updateUserCountTable( connection:Connection, table: String, user: Int, ad: Int, count: Int ) = {
//    val statement: PreparedStatement = connection.prepareStatement(
//      s"""
//         |update ${table}
//         |set count = ?
//         |where user = ?
//         |and ad = ?
//         |""".stripMargin)
//    statement.setInt(1, count)
//    statement.setInt(2, user)
//    statement.setInt(3, ad)
//
//    statement.executeUpdate()
//    statement.close()
//    connection.close()
//  }
  //INSERT INTO table (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
  def updateUserCountTableIgnoreExists( connection:Connection,table: String, user: Int, ad: Int, count: Int ): Unit = {
    val statement: PreparedStatement = connection.prepareStatement(
      s"""
         |INSERT INTO ${table}
         |(user,ad,count) VALUES (?,?,?)
         |ON DUPLICATE KEY
         |UPDATE count = ?
         |""".stripMargin)
    statement.setInt(1, user)
    statement.setInt(2, ad)
    statement.setInt(3, count)
    statement.setInt(4, count)

    statement.executeUpdate()
    statement.close()
  }

  def getKafkaStream( sc: StreamingContext, topic: String ): InputDStream[ConsumerRecord[String, String]] = {
    var kafkaProps: Map[String, String] = Map[String, String]()
    kafkaProps += ("bootstrap.servers" -> "192.168.10.100:9092,192.168.10.101:9092,192.168.10.102:9092")
    kafkaProps += ("group.id" -> "blackListNew3")
    kafkaProps += ("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps += ("value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps += ("enable.auto.commit" -> "true")


    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(List(topic), kafkaProps)
    )
    stream
  }

}
