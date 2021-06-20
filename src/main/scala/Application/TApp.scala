package Application

import Controller.ControllerWordCount
import Util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApp {


  def start(master:String="local[*]",app:String="application")( op: => Unit ): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(app).setMaster(master)
    val sc = new SparkContext(conf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case e => e.printStackTrace()
    } finally {
      sc.stop()
    }

  }

}
