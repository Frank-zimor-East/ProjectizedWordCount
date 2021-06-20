import Streaming.GetConnections
import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

object Test2 {

  def initConnection() :Connection  = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://localhost:3306")
    properties.setProperty("username", "root")
    properties.setProperty("password", "000000")
    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    dataSource.getConnection
  }

  def readUserCountTable(table:String,user:Int,ad:Int) ={
    val connection: Connection = initConnection()
    val statement : PreparedStatement = connection.prepareStatement(
      s"""
         |select count
         |from ${table}
         |where user =?
         |and ad =?
         |""".stripMargin)
    statement.setInt(1,user)
    statement.setInt(2,ad)

    val set: ResultSet = statement.executeQuery()
    while(set.next()){
      println(set.getInt(1))
    }

    connection.close()

  }

  def main( args: Array[String] ): Unit = {
    val connection: Connection = GetConnections.initConnection()
    val userCountTable = "spark.user_ad_count"
    GetConnections.updateUserCountTableIgnoreExists(connection,userCountTable,3,3,3)
  }



}
