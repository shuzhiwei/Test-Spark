package cn.tedu.sgd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

/**
 * 用梯度下降法建立回归模型, 预测数据
 * 知识点
 * 1 批量梯度下降法: 每次更新系数时,所有的样本都需要参与计算
 * 		优点: 需要很少的迭代次数,就可以收敛
 * 		缺点: 在数据量较大时,更新一次所需要的时间会非常长
 * 		在生产环境下,很少使用批量梯度下降法 
 * 2 随机梯度下降法: 每次在更新系数时,从所有的样本中随机选择一个样本参与计算
 * 		优点: 更新一次所需要的时间很短
 * 		缺点: 需要更多的迭代次数才能收敛
 * 		在生产环境下,是用随机梯度下降法来解模型
 * 本例中用的就是随机梯度下降法(SGD), (全称Stochastic Gradient Descent)
 * 
 * 
 */

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sgd")
    val sc = new SparkContext(conf)
    val data = sc.textFile("d://data/ml/testSGD.txt")
    
    //--RDD[String]->RDD[LabeledPoint(Y,Vectors(X1,X2..))]
    val r1 = data.map{line =>
      val info = line.split(",")  
      val Y = info(0).toDouble
      val featuresArray = info(1).split(" ").map{num => num.toDouble}
      
      LabeledPoint(Y,Vectors.dense(featuresArray))
    }
    
    //--①:数据集
    //--②:迭代次数
    //--③:步长; 步长过小,会导致迭代了很多还没有收敛; 过大,会导致围绕真实解震荡而不收敛; 步长的选取一般: 0.01~0.5
    val model = LinearRegressionWithSGD.train(r1,20,0.05)
    
    //--获取模型的自变量系数
    //--Y=β1X1+β2X2
    val coef = model.weights
    //println(coef)
    
    //--获取截距项系数
    val intercept = model.intercept
    //println(intercept)
    
    //通过模型预测
    val predict = model.predict(r1.map{x=>x.features})
    
    predict.foreach{println}
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}