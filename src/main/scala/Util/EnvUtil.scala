package Util

import org.apache.spark.{SparkConf, SparkContext}

object EnvUtil extends ThreadLocal[SparkContext]{

  var sc: SparkContext = null

  override def get()={
    sc
  }

  def put(sc:SparkContext)={
    this.sc = sc
  }


}
