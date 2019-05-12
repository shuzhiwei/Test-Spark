package cn.tedu.median

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
1 20 8 2 5 11 29 10
7 4 45 6 23 17 19
求中位数
 */
object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("median")
    val sc = new SparkContext(conf)
    
    /*val data = sc.textFile("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/median.txt")
    val r1 = data.map{x=>x.split("\r\n")}.map{arr => arr.mkString(" ")}
    .flatMap{_.split(" ")}.sortBy{x=> -x.toInt}.take(8).reverse.take(1)
    r1.foreach{println}*/
    
    

      
    
    
  }
}