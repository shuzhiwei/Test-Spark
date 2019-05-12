package cn.tedu.vector

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 用于学习MLlib的向量类型
 * 因为很多模型要求数据类型必须是Vector才能处理
 * 学习密集型向量
 */

object Driver01 {
  def main(args: Array[String]): Unit = {
    //--创建密集型向量, 元素类型是Double
    val v1 = Vectors.dense(1.2,3.1,4.1)
    //--如果传入的是Int类型, 底层会通过隐式转换, 变成Double
    val v2 = Vectors.dense(1,2,3,4)
    
    //--重点掌握下面的形式: 通过Array[Double]创建
    val v3 = Vectors.dense(Array(1.1,2.2,3.5))
    
    println(v3)
    
    val conf = new SparkConf().setMaster("local").setAppName("ml")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("d://data/ml/vectors.txt")
    
    val r1 = data.map{line=>
      line.split(" ").map{num=>num.toDouble}
    }.map{arr => Vectors.dense(arr)}
    
    r1.foreach{println}
    
  }
}





























