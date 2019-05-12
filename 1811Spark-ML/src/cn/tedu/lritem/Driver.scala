package cn.tedu.lritem

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

/**
 * 建立回归模型, 预测商品的需求量
 * 
 * 概念总结:
 * 多元线性回归模型
 * 1. 什么是多元? 既有多个自变量(X1,X2,X3...),即如果自变量的个数>1个, 都称为多元
 * 2. 什么是线性?指的是方程的类别, 即线性方程, 比如:
 * 直线方程,平面方程以及超平面方程都是线性方程
 * 直线方程的表达式: y=β1X1+β0
 * 平面方程的表达式: y=β1X1+β2X2+β0
 * 超平面方程的表达式: y=β1X1+β2X2+...+βnXn+β0
 * 3. 什么是回归模型?回归模型的应用场景就是预测结果,建立回归模型其实就是在求解回归方程
 * (求系数), 当把方程得到之后, 就可以通过回归方程预测数据
 * 4. 如何求解回归方程的系数?①最小二乘法 ②梯度下降法
 * 
 * 问: 一元线性回归模型的表达式?
 * y=β1X1+β0
 * 
 * 补充:建立回归方程,也可以是非线性的
 * 既有: 一元非线性方程, 多元非线性方程
 * 但是: 在一般情况下, 都是用线性方程去拟合的, 因为线性方程的形式固定, 求解方便
 * 而非线性方程的形式未知, 求解难度大
 * 有些场景, 在原空间中如果通过线性方程效果不好, 可以通过特征变化的手段(比如升维),
 * 在高维的特征空间中, 用线性方程解决(比如SVM支持向量机算法)
 */

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ml")
    val sc = new SparkContext(conf)
    val data = sc.textFile("d://data/ml/lritem.txt")
    
    val r1 = data.map{line =>
      val info = line.split("\\|")  
      val Y = info(0).toDouble
      val X1 = info(1).split(" ")(0).toDouble
      val X2 = info(1).split(" ")(1).toDouble
      (X1,X2,Y)
    }
    
    //
    val sqc = new SQLContext(sc)
    val t1 = sqc.createDataFrame(r1).toDF("X1","X2","Y")
    //创建向量格式转化器工具, 并设定所有的自变量的列名以及为所有的自变量分配一个别名
    val ass = new VectorAssembler().setInputCols(Array("X1","X2"))
                  .setOutputCol("features")
    
    val t2 = ass.transform(t1)
    
    //--setFeaturesCol:设定自变量列
    //--setFitIntercept: true表示要求模型计算截距项系数
    //--setLabelCol: 设定因变量列
    //--fit: 代入数据集建模
    val model = new LinearRegression().setFeaturesCol("features")
                  .setFitIntercept(true)
                  .setLabelCol("Y")
                  .fit(t2)
    
    //y=β1X1+β2X2+β0
    //--获取模型自变量的系数
    val coef = model.coefficients 
    //println(coef)
    //--获取截距项系数,这个系数不一定需要解,所有有些模型底层不计算该系数
    val intercept = model.intercept
    //println(intercept)
    
    //--获取模型的多元R方值, 这个指标用于衡量模型的拟合优良性
    //--此值最大值=1, 越趋近于1, 说明模型对于数据拟合的越好
    //--在生产环境下, 0.5以上都可以接受
    val R2 = model.summary.r2
    
    //--通过模型实现预测,要求传入的数据集类型:RDD->DataFrame->Vector工具处理后的DataFrame
    //--在预测时, 模型是根据自变量实现预测,和因变量Y没有关系
    val predictResult = model.transform(t2)
    predictResult.show
    
    
    //--现在要求预测一组数据: X1=10,X2=310,预测Y=?
    /*val testRDD = sc.makeRDD(List((10,310,0)))
    val testDF = sqc.createDataFrame(testRDD).toDF("X1","X2","Y")
   
    val testDFVector = ass.transform(testDF)
    
    val predictTest = model.transform(testDFVector)
    
    predictTest.show*/
    
    val data1 = sc.textFile("D://data/ml/testitem.txt")
    val testRDD = data1.map{line =>
       val info = line.split(" ")  
       (info(0).toDouble,info(1).toDouble,0)
    }
    val testDF = sqc.createDataFrame(testRDD).toDF("X1","X2","Y")
    val testDFVector = ass.transform(testDF)
    val predictTest = model.transform(testDFVector)
    predictTest.show
    
  }
}





















