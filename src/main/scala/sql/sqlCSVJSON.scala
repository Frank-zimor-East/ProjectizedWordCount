package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object sqlCSVJSON {
  def main( args: Array[String] ): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = spark.read.json("H:\\ProjectizedWordCount\\src\\main\\resources\\persons.json")
    df.show()


    println("=======================")
    val df2: DataFrame = spark.read
      .option("header", true)
      .csv("H:\\ProjectizedWordCount\\src\\main\\resources\\users.csv")
    df2.show()



    println("========================")

    spark.stop()
  }
}
