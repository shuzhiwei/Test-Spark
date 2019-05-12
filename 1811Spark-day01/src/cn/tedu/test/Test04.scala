package cn.tedu.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *4.处理topk.txt文件,得到频次最高的前三项单词
比如:
(hive,10)
(hadoop,8)
(world,4) 

hello world bye world
hello hadoop bye hadoop
hello world java web
hadoop scala java hive
hadoop hive redis hbase
hello hbase java redis
 *
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test04")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/topk.txt")
   // val data1 = data.flatMap{_.split(" ")}.map{(_,1)}.reduceByKey(_+_).sortBy{x=> -x._2}.take(3)
    
    val data1 = data.flatMap{_.split(" ")}.map{(_,1)}.reduceByKey{_+_}
    /*val orderingDesc = Ordering.by[(String,Int),Int](_._2)
    val topK = data1.top(3)(orderingDesc)*/
    
    val topK = data1.top(3)(Ordering.by{case (k,v) => v})
    
    topK.foreach{println}
    
  }
}