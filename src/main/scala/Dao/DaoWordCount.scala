package Dao

import Util.EnvUtil
import org.apache.spark.rdd.RDD

class DaoWordCount extends TDao {

  def getFile(path: String ): RDD[String] = {
    EnvUtil.get().textFile(path)
  }


}
