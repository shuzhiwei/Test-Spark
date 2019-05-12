package cn.tedu.vector

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 学习向量标签类型
 */

object Driver03 {
  def main(args: Array[String]): Unit = {
    val v1 = Vectors.dense(1,2,3)
    //--①:向量的标签值 因变量
    //--②:向量
    val lb1 = LabeledPoint(50.0,v1)
    println(lb1)
    
    val conf = new SparkConf().setMaster("local").setAppName("ml")
    val sc = new SparkContext(conf)
    val data = sc.textFile("d://data/ml/labeled.txt")
    
    /*val r1 = data.map{line =>
      line.split(" ").map{num => num.toDouble}  
      
    }.map{arr =>
       val v2 = Vectors.dense(arr(0),arr(1)) 
       LabeledPoint(arr(2),v2)
    }
    
    r1.foreach{println}*/
    
    val r1 = data.map{line=>
      val info = line.split(" ")  
      //--获取所有的自变量
      val featureCols = info.dropRight(1).map{num => num.toDouble}
      //--获取标签列(因变量)
      val labelCols = info.last.toDouble
      
      LabeledPoint(labelCols, Vectors.dense(featureCols))
    }
    
    r1.foreach{println}
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}