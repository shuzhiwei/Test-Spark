package cn.tedu.als

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object LoadDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("load")
    val sc = new SparkContext(conf)
    
    //--执行模型的加载
    val model = MatrixFactorizationModel.load(sc, "d://data/ml/alsmovie")
    
    val u500 = model.recommendProducts(500, 5)
    //u500.foreach{println}
    
    //--Spark ALS的推荐系统, 只能完成基于用户的推荐
    //--即此算法底层没有实现基于物品的推荐, 需要用户自己实现
    //--什么是物品推荐: 比如一个用户看了一部电影(123), 然后要求模型推荐和123号电影相关的电影
    
    //--要求: 通过模型, 基于123号电影, 推荐10部相关电影
    //--①要计算出其他所有电影和123号电影的相似度->难点
    //--②根据相似度降序排序, 取出前10部
    
    //--获取用户因子矩阵, 通过它可以计算用户之间的相似度
    val userFactor = model.userFeatures
    
    //--获取物品的因子矩阵, 用于计算物品之间的相似度
    //--RDD[(Int:物品id,Array[Double]:物品的因子值)]
    val productFactor = model.productFeatures
    
    //--获取123号电影的因子值数据
    val movie123Factor = productFactor.keyBy{x=>x._1}.lookup(123).head._2
    
   // movie123Factor.foreach{println}
    
    val movieCos = productFactor.map{x=>
      val movieId = x._1
      val otherMovieFactor = x._2
      val cosResult = cosArray(movie123Factor, otherMovieFactor)
      (movieId, cosResult)
    }
    
    //movieCos.foreach{println}
    
    val recommend123Top10 = movieCos.sortBy{x=> -x._2}.take(10)
    
    recommend123Top10.foreach{println}
    
    
    
    
    
    
    
    
  }
  
  def cosArray(a1:Array[Double],a2:Array[Double])={
    val a1a2 = a1 zip a2
    val a1a2Fenzi = a1a2.map{x=>x._1*x._2}.sum
    val a1Fenmu = Math.sqrt(a1.map{x=>x*x}.sum)
    val a2Fenmu = Math.sqrt(a2.map{x=>x*x}.sum)
    val a1a2Cos = a1a2Fenzi/(a1Fenmu*a2Fenmu)
    a1a2Cos
  }
  
}


















