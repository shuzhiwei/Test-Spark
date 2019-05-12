package cn.tedu.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  def main(args: Array[String]): Unit = {
    //创建Spark的环境从参数对象
    //最基本的设置运行模式和jobId
    //val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val conf = new SparkConf().setMaster("spark://cloud1:7077").setAppName("wordcount1")
    //创建Spark的上下文对象
    //是操作Spark环境的入口对象
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("hdfs://cloud1:9000/word.txt", 2)
    
    //--处理data, 统计单词频次, 最终将结果存储到HDFS /wordresult目录下
    val result = data.flatMap{_.split(" ")}.map{(_,1)}.reduceByKey{_+_}
    result.coalesce(1, true).saveAsTextFile("hdfs://cloud1:9000/wordresult02")
    
  }
}