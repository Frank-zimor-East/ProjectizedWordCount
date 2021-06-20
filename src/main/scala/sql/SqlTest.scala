package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SqlTest {
  def main( args: Array[String] ): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    //RDD - DF
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jack", 18), ("Rose", 16), ("Lee", 20)))
    val df: DataFrame = rdd.toDF()
    df.printSchema()
    df.show()
    val df2: DataFrame = rdd.toDF("name", "age")
    df2.printSchema()
    df2.show()

    //DF- RDD
    val df_rdd: RDD[Row] = df.rdd
    df_rdd.foreach(println)
    val df_rdd2: RDD[Row] = df2.rdd
    df_rdd2.foreach(println)


    //DF -DS
    //    val ds: Dataset[Person] = df.as[Person]
    //    ds.show
    val ds: Dataset[Person] = df.select('_1 as "name", '_2 as "age").as[Person]
    ds.show
    val ds2: Dataset[Person] = df2.as[Person]
    ds2.show
    //DS- DF
    println("==========ds_df=========")
    val ds_df: DataFrame = ds.toDF()
    ds_df.show()
    println("==========ds_df2=========")
    val ds_df2: DataFrame = ds2.toDF()
    ds_df2.show()

    //df table
    println("-=============table==========")
    ds_df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    spark.sql(
      """
        |SELECT NAME,
        |AGE + 1 AS AGE_PLUS_1
        |FROM USER
        |""".stripMargin).show

    ds_df.select('name, 'age + 1 as "new_age" ) .show
    ds_df.select('name , 'age + 1  as "new_age") .show

    //RDD - DS
    println("==========rdd_ds=========")
    rdd.toDS().show()
    //DS - RDD


    spark.stop()
  }

}

case class Person( name: String, age: Int )
