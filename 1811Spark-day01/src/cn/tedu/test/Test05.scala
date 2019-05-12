package cn.tedu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ssort")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("C:\\Users\\000\\Desktop\\aaa.txt",2)
    
    val r1 = data.map{x=>
      val mySSort = new MySsort(x.split(" ")(0), x.split(" ")(1).toInt)
      (mySSort,x)
    }
    
    val r2 = r1.sortByKey(true).map{_._2}
    r2.foreach{println}
    
  }
}