package cn.tedu.murder

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object Driver02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("muder02")
    val sc = new SparkContext(conf) 
    
    val data = sc.textFile("d://data/ml/lrmurder-test.txt")
    val r1 = data.map{line =>
      line.split(",").map{num => num.toDouble}  
    }.map{arr => 
      (arr(1), arr(2), arr(3), arr(5), arr(6), arr(4))  
    }
    val sqc = new SQLContext(sc)
    val t1 = sqc.createDataFrame(r1).toDF("X1","X2","X3","X4","X5","Y")
    val ass = new VectorAssembler().setInputCols(Array("X1","X2","X3","X4","X5"))
              .setOutputCol("features")
    val t2 = ass.transform(t1)
    
    val model = new LinearRegression().setFeaturesCol("features")
              .setFitIntercept(true)
              .setLabelCol("Y")
              .fit(t2)
    val R2 = model.summary.r2
    println(R2)//0.2257341822414134
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
}