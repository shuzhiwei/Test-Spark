package cn.tedu.check

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("check")
    val sc = new SparkContext(conf)
    
    //--设置检查点目录路径,可以本地,也可以是hdfs
    sc.setCheckpointDir("d://data//check")
    
    val data = sc.textFile("E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/topk.txt")
    
    data.cache()
    
    val r1 = data.flatMap{_.split(" ")}
    val r2 = r1.map{(_,1)}
    r2.cache()
    //--吧指定的RDD存储在检查点文件, 当缓存丢失, 可以从文件中恢复
    r2.checkpoint()
    
    val r3 = r2.reduceByKey{_+_}
    
    r3.foreach{println}
    
    data.unpersist()
    r2.unpersist()
    
  }
}