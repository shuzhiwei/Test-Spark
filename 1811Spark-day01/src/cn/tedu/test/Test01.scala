package cn.tedu.test
/**
 * 1.处理average.txt文件,统计出第二列的均值
 	1 16
	2 74
	3 51
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("file:///E:/舒志伟/资料/Spark第一天/Spark第一天/作业文件/average.txt",3)
    val r1 = data.map{x=>
      x.split(" ")(1).toInt  
    }
    
    val r2 = r1.mapPartitions{x=>
      val list = List[Int]()  
      var sum = 0
      while(x.hasNext){
        sum += x.next
      }
      list.::(sum).iterator
    }.reduce{_+_}
    println(r2)
    
    /*//RDD[String]->RDD[Array[String]]->RDD[Int]
    val data1 = data.map{x=>
      val key=x.split(" ")(0)  
      val value=x.split(" ")(1).toInt
      value
    }
    val sum = data1.reduce(_+_)
    val count = data1.count()
    val avg = sum.toInt / count
    println(avg)*/
    
    /*val r1 = data.map{x=>
      x.split(" ")(1).toInt
    }
    
    val r2 = r1.mapPartitions{ x=>
      val list = List[Int]()
      var sum = 0
      while(x.hasNext){
        sum+=x.next
        
      }
      list.::(sum).iterator
    }*/
    
   // val r1 = data.map{x=>x.split(" ")(1).toInt}.reduce{_+_}
    
    
    
    
  }
}