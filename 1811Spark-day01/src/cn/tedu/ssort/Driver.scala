package cn.tedu.ssort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ssort")
    val sc = new SparkContext(conf) 
    
    val data = sc.textFile("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/ssort.txt",2)
    
    val r1 = data.map{line => {
        
        val col1 = line.split(" ")(0)
        val col2 = line.split(" ")(1).toInt
        val ssort = new Ssort(col1, col2)
        (ssort, line)
      }
    }
    
    val r2 = r1.sortByKey(true).map{x=>x._2}
    r2.foreach{println}
    
    
    
  }
  
  
}