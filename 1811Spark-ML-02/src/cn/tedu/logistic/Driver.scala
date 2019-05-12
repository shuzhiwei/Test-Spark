package cn.tedu.logistic
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
/**
 * 用逻辑回归模型, 解决因变量是离散型数据的情况
 * 1 逻辑回归用于解决二分类问题, 不同于其他的回归模型(预测), 而是分类
 * 2 因为分类问题, 因变量Y都是离散的, 比如动物分类(猫,狗,..); 逻辑回归底层
 * 用的函数是: Sigmoid
 * Sigmoid函数的作用: 可以把连续型数据离散化
 */

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("logistic")
    val sc = new SparkContext(conf)
    val data = sc.textFile("d://data/ml/logistic.txt")
    
    //--RDD[String]->RDD[LabeledPoint]
    val r1  = data.map{line=>
      val info = line.split("\t")  
      val Y = info.last.toDouble
      val featuresArray = info.dropRight(1).map{num => num.toDouble}
      LabeledPoint(Y,Vectors.dense(featuresArray))
    }
    
    //--建立逻辑回归模型, 底层用的是随机梯度下降法求解系数
    //val model = LogisticRegressionWithSGD.train(r1, 20, 0.013)
    
    //--建立逻辑回归模型, 底层用的是牛顿法来求解系数
    //--优点: 需要较少的迭代次数就可以收敛
    //--缺点: 耗费较大的计算资源
    val model = new LogisticRegressionWithLBFGS().run(r1)
    
    
    //--获取自变量系数
    val coef = model.weights
    //println(coef)
    //--获取截距项系数
    val intercept = model.intercept
    //println(intercept)
    
    //--回代原数据集, 实现分类(1或0)
    val result = model.predict(r1.map{x=>x.features})
    //result.foreach{println}
    
    
    val data1 = sc.textFile("d://data/ml/testlogistic.txt")
    val r2 = data1.map{line=>
      val info = line.split(" ").map{num => num.toDouble}
      Vectors.dense(info)
    }
    val result1 = model.predict(r2)
    result1.foreach{println}
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}