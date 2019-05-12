package cn.tedu.cache

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cache")
    val sc = new SparkContext(conf) 
    
    val data = sc.textFile("hdfs:cloud1:9000/word.txt",2)
    //一般将数据集进行缓存, 避免重新计算时读取磁盘文件
    //数据集的缓存不是集中在一台服务器上, 是多台服务器共同维护的
    data.cache()
    //data.persist()等同于data.cache(), 缓存级别都是: MEMORY_ONLY
    //此外, persist()还可以指定具体的缓存机制, 而cache()只有一种缓存级别
    val r1=data.flatMap{_.split(" ")}
    
    //--在生产环境下, 一个DAG可能有很多RDD,可以选择一些重要的处于中间部分的RDD做缓存
    //--这样可以降低重新计算带来的计算代价
    //--比如: 整个DAG共有30个RDD,我们可以选择每隔5个RDD缓存一次
    val r2=r1.map{(_,1)}.persist(StorageLevel.MEMORY_AND_DISK)
    val r3=r2.reduceByKey{_+_}
    r3.foreach{println}
    
    //一定要释放缓存
    data.unpersist()
    r2.unpersist()
  }
}