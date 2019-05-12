package cn.tedu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 3.处理MaxMin.txt文件,得到男性身高最大值的那一行数据 8 M 191
1 M 174
2 F 165
3 M 172
4 M 180
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test03")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("file:///E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/MaxMin.txt")
    /*val data1 = data.map{x=>
      val id = x.split(" ")(0)  
      val sex = x.split(" ")(1)
      val high = x.split(" ")(2)
      
      (sex, id+"_"+high)
    }.groupByKey.filter { x => if(x._1 == "M") true else false}.foreach{x=>
      val sexM = x._1
      val idM = x._2.toList.map{x=>
        val id = x.split("_")(0)  
        val highM = x.split("_")(1)
        (id, highM)
      }.sortBy{x=>x._2}.reverse.take(1)
      println(idM, sexM)
    }*/
    
    //第二种解法
    /*val r1 = data.filter{line => line.split(" ")(1).equals("M")}
          .sortBy{line => -line.split(" ")(2).toInt}.first
    println(r1)*/
    
    //第三种解法
    val r1 = data.filter{line => line.split(" ")(1).equals("M")}
        .map{line => line.split(" ")}
        .sortBy{arr => -arr(2).toInt}
        .map{arr => arr.mkString(",")}
        .take(1)
        
    r1.foreach{println}
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
  
  
  
  
}