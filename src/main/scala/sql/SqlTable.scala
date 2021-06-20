package sql


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions, types}

case class Sensor(name:String,time:String,temperature:Double)

object SqlTable {
  def main( args: Array[String] ): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "test.sensor_temp")
      .option("user", "root")
      .option("password", "000000")
      .load()


    df.show()

    df.as[Sensor].select('name,'time,'temperature + 100 as "temperature" )
      .write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "test.sensor_temp2")
      .option("user", "root")
      .option("password", "000000")
      .mode("append")
      .save()


    println("========================")








    spark.stop()
  }
}

