package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object UDAFAggregator {
  def main( args: Array[String] ): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    spark.udf.register("myAvg", new MyAvg)

    val rdd: RDD[(String, Long)] = sc.makeRDD(List(("jack", 18L), ("Rose", 16L), ("Lee", 20L)))
    val df: DataFrame = rdd.toDF("name", "age")
    df.createOrReplaceTempView("user")

    spark.sql("select myAvg(age) from user").show()


    spark.stop()
  }
}

class MyAvg extends UserDefinedAggregateFunction {

  //输入数据类型,
  override def inputSchema: StructType = new StructType().add("age", LongType)

  //缓冲区数据类型
  override def bufferSchema: StructType = {
    StructType(List(
      StructField(name = "total", LongType),
      StructField(name = "cnt", LongType)
    ))
  }

  //返回结果数据类型
  override def dataType: DataType = DoubleType

  //是否有随机性, 同样输入 一直同样输出
  override def deterministic: Boolean = true

  //初始化
  override def initialize( buffer: MutableAggregationBuffer ): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 分区内新增row 处理
  override def update( buffer: MutableAggregationBuffer, input: Row ): Unit = {
    val value: Long = input.getLong(0)
    val total: Long = buffer.getLong(0)
    val cnt: Long = buffer.getLong(1)

    buffer.update(0, total + value)
    buffer.update(1, cnt + 1)
  }

  // 分区间合并
  override def merge( buffer1: MutableAggregationBuffer, buffer2: Row ): Unit = {
    val total: Long = buffer1.getLong(0) + buffer2.getLong(0)
    val cnt: Long = buffer1.getLong(1) + buffer2.getLong(1)

    buffer1.update(0, total)
    buffer1.update(1, cnt)

  }


  //函数返回值计算
  override def evaluate( buffer: Row ):Any= {
    val d: Double = buffer.getLong(0) * 1.0D / buffer.getLong(1)
    d
  }
}
