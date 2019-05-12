package cn.tedu.test2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test02")
    val sc = new SparkContext(conf)
    
    val data = sc.makeRDD(List((1,"北京/天津"),(2,"上海/西安/南京"),(3,"广西/深圳"),(4,"沈阳/长春")))
    
    val r1 = data.map{x=>
      x._2.split("/").map{a => (x._1,a)}
      .mkString("\n")  
    }
    
    r1.foreach{println}
    
   
    
  }
}