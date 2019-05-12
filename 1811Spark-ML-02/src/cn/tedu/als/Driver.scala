package cn.tedu.als

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

/**
 * 通过Spark MLlib提供的ALS算法建立推荐系统模型,完成电影推荐
 */

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("als")
    val sc = new SparkContext(conf)   
    
    val data = sc.textFile("d://data/ml/u.data",4)
    
    //--为了满足ALS算法建模需求,要求传入的数据类型: RDD[Rating(userId,ItemId,score)]
    //--RDD[String]->RDD[Rating]
    val parseData = data.map{line=>
      val info = line.split("\t")  
      val userId = info(0).toInt
      val itemId = info(1).toInt
      val score = info(2).toDouble
      Rating(userId, itemId, score)
    }
    
    //--①:数据集
    //--②:隐藏因子k的数量
    //--③:最大的迭代次数
    //--④:λ 正则化参数,防止模型过拟合
    val model = ALS.train(parseData, 50, 10, 0.01)
    
    
    
    //
    val i50 = model.recommendUsers(50, 5)
    //i50.foreach{println}
    
    //--接下来要将id变成电影名
    val movieData = sc.textFile("d://data/ml/u.item",3)
    val movieMap = movieData.map{line=>
      val info = line.split("\\|")
      val movieId = info(0).toInt
      val movieName = info(1)
      (movieId,movieName)
    }.collectAsMap
    
    //println(movieMap(688))
    
    //--表示为789号用户推荐10部电影
    val u789 = model.recommendProducts(789, 10)
              .map{x=>
                val userId = x.user
                val itemId = x.product
                val score = x.rating
                (userId, itemId, movieMap(itemId),score)
    }
    //u789.foreach(println)
    
    //--接下来,做模型的验证, 做直观方式的验证
    //--①获取789号用户看过的所有电影
    //--②根据789号用户对电影的打分, 找出他最喜爱的前10部电影
    //--③用推荐的10部电影和他喜爱的10部电影比对, 比较是否有相类似的电影
    
    //--keyBy指定以什么属性为查找的key
    //--lookup指定找哪个key, 下面的代码表示找到789号为key的所有数据
    val u789Movies = parseData.keyBy{x=>x.user}.lookup(789)
    
    //u789Movies.foreach{println}
    
    val u789Top10 = u789Movies.sortBy{x=> -x.rating}.take(10)
        .map{x=>
          val userId = x.user
          val itemId = x.product
          val score = x.rating
          val movieName = movieMap(itemId)
          (userId, itemId, movieName, score)
        }
    
    //模型训练好之后,要进行模型的存储,可以存到本地,也可以存到HDFS
    model.save(sc,"d://data/ml/alsmovie")
    
    
    
    
    
  }
}



















