package cn.tedu.kryo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
              .setAppName("kryo")
              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
              .set("spark.kryo.registrator","cn.tedu.MyKryoRegister")
    
    val sc = new SparkContext(conf)
    
    val p1 = new Person
    p1.name="tom"
    p1.age=23
    
    val p2=new Person
    p2.name="rose"
    p2.age=24
    
    val r1=sc.makeRDD(List(p1,p2),2)
    r1.persist(StorageLevel.MEMORY_ONLY_SER)
    r1.unpersist()
  }
}