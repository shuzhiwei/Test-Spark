package cn.tedu.statistic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
/**
 * 学习MLlib提供的统计工具类
 */

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ml")
    val sc = new SparkContext(conf)
    
    val r1 = sc.makeRDD(List(1.1,2.3,4.5,5.6))
    
    //--RDD[Double]-->RDD[Vectors]
    val v1 = r1.map{num => Vectors.dense(num)}
    
    val result = Statistics.colStats(v1)
    println(result.max)
    println(result.min)
    println(result.mean)//平均值
    println(result.count)
    println(result.variance)//方差
    println(result.normL1)//获取曼哈顿距离
    println(result.normL2)//获取欧式距离: 空间中两点的直线距离; 衡量相似度: 距离越小,相似度越高
    
  }
}