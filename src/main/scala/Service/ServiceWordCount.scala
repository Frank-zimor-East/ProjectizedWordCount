package Service

import Dao.DaoWordCount
import org.apache.spark.rdd.RDD

class ServiceWordCount extends TService {
  val daoWordCount = new DaoWordCount

  override def wordCount( ) = {
    // Dao层获取数据
    val path: String = "H:\\ProjectizedWordCount\\src\\main\\resources\\words.txt"
    val file: RDD[String] = daoWordCount.getFile(path)
    //处理逻辑
    file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
  }


}
