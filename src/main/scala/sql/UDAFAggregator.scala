package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions, types}

object UDAFDemo {
  def main( args: Array[String] ): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._



    val rdd: RDD[(String, Long)] = sc.makeRDD(List(("jack", 18L), ("Rose", 16L), ("Lee", 20L)))
    val df: DataFrame = rdd.toDF("name", "age")
    df.createOrReplaceTempView("user")

    spark.udf.register("myAvg",functions.udaf(new MyAvgAggregator ))

    spark.sql("select myAvg(age) from user").show()


    spark.stop()
  }
}

class MyAvgAggregator extends Aggregator[Int, Buff, Double] {

  override def zero: Buff = Buff(0,0)

  override def reduce( b: Buff, value: Int ): Buff = {
    b.total = b.total + value
    b.cnt = b.cnt + 1
    b
  }

  override def merge( b1: Buff, b2: Buff ): Buff = {
    b1.total = b1.total + b2.total
    b1.cnt = b1.cnt + b2.cnt
    b1
  }

  override def finish( reduction: Buff ): Double = reduction.total.toDouble / reduction.cnt

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
case class Buff(var total:Long,var cnt:Long)