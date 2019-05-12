package cn.tedu.murder

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("murder")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("d://data/ml/lrmurder-sample.txt")
    
    val r1 = data.map{line =>
      val info = line.split(",")  
      (info(0).toDouble, info(1).toDouble, info(2).toDouble, info(3).toDouble, info(5).toDouble, info(6).toDouble, info(7).toDouble, info(4).toDouble)
    }
    val sqc = new SQLContext(sc)
    val t1 = sqc.createDataFrame(r1).toDF("X1","X2","X3","X4","X5","X6","X7","Y")
    
    //t1.show()
    
    val ass = new VectorAssembler().setInputCols(Array("X1","X2","X3","X4","X5","X6","X7"))
                            .setOutputCol("features")
    
    val t2 = ass.transform(t1)
    
    val model = new LinearRegression().setFeaturesCol("features")
                .setFitIntercept(true)
                .setLabelCol("Y")
                .fit(t2)
    val coef = model.coefficients
    //println(coef)
                
    val R2 = model.summary.r2
    println(R2)//0.8082607280926754
    
    
    //预测
    val dataText = sc.textFile("d://data/ml/lrmurder-test.txt")
    val testRDD = dataText.map{line =>
      line.split(",").map{_.toDouble} 
    }.map{arr =>
      (arr(0), arr(1), arr(2), arr(3), arr(5), arr(6), arr(7), 0.0)
    }
    
    val testDF = sqc.createDataFrame(testRDD).toDF("X1","X2","X3","X4","X5","X6","X7","Y")
    val testDFVector = ass.transform(testDF)
    val predictTest = model.transform(testDFVector)
    predictTest.show()
    
    
    
    
    
    
    
    
    
                
                
                
                
                
  }
}