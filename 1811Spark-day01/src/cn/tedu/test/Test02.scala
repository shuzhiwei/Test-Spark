package cn.tedu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 2.处理MaxMin.txt文件,得到男性身高的最大值(191)
1 M 174
2 F 165
3 M 172
4 M 180
 */
object Test02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test02")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/MaxMin.txt")
    /*val data1 = data.map{x=>
      val sex = x.split(" ")(1)  
      val high = x.split(" ")(2)
      (sex,high)
    }.groupByKey().filter{x=>if(x._1=="M") true else false}.foreach{x=>
      val arr = x._2
      val list = arr.toList.sorted.reverse
      println(list(0))
      
    }*/
    
    //第二种解法
    //先过滤出男性数据,在将身高列单独取出来
    /*val r1=data.filter{line=>line.split(" ")(1).equals("M")}
      .map{line => line.split(" ")(2).toInt}
      
    val max = r1.max()
    println(max)
    */
    
    val r1 = data.filter{x=>x.split(" ")(1).equals("M")}.sortBy(x=>x.split(" ")(2).toInt,false).take(1)
      .map{_.split(" ")(2)}.mkString
    r1.foreach{println}
    
    
  }
}