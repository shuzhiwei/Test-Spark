package cn.tedu.invert

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("invert")
    val sc = new SparkContext(conf)
    
    //val data = sc.wholeTextFiles("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/inverted", 2)
    //val data = sc.textFile("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/inverted/doc1.txt", 2)
    //data.foreach{println}
    
    val data1 = sc.textFile("c://Users/000/Desktop/aaa.txt",1)
    val data2 = sc.textFile("c://Users/000/Desktop/bbb.txt",1)
    
    val d1 = data1.map{x=>(x.split(" ")(0),x.split(" ")(1).toInt)}.reduceByKey{_+_}
    val d2 = data2.map{x=>(x.split(" ")(0),x.split(" ")(1).toInt)}.reduceByKey{_+_}
   
    val r1 = d1.union(d2).reduceByKey{_+_}
    
    
    r1.saveAsTextFile("c://Users/000/Desktop/result")
 
    
    
    
    /*val r1 = data.map{x=>
      val fileName = x._1.split("/").last.dropRight(4) 
      (fileName, x._2)
    }
    
    val r2 = r1.flatMap{case (fileName,fileText)=>
       fileText.split("\r\n").flatMap{_.split(" ")}.map{(_,fileName)}
    }
    
    val result = r2.groupByKey.map{case(word,it) => (word,it.toList.distinct.mkString("、"))}
    result.foreach{println}*/
    
  }
}