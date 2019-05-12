package cn.tedu.vector

import org.apache.spark.mllib.linalg.Vectors

/**
 * 学习稀疏向量
 */

object Driver02 {
  def main(args: Array[String]): Unit = {
    //--①:向量的元素的个数 
    //--②:指定向量的下标
    //--③:指定下标对应的值
    //--稀疏向量: 只有指定的下标有值, 未指定的下标的值都是零
    val v1 = Vectors.sparse(7, Array(0,2,5), Array(1.5,4.3,1.1))
    
    println(v1(2))
  }
}