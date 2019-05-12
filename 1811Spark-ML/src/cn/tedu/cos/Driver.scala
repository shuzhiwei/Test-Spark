package cn.tedu.cos

object Driver {
   def main(args: Array[String]): Unit = {
     val a1 = Array(1,2,3)
     
     val a2 = Array(3,5,8)
     
     //计算a1和a2之间的余弦距离
     
     println(cosArray(a1,a2))
     
     
   }
   
   def cosArray(a1:Array[Int],a2:Array[Int])={
     val a1a2 = a1 zip a2
     
     val fenzi = a1a2.map{x=>x._1*x._2}.sum
     
     val a1Fenmu = Math.sqrt(a1.map{num=>num*num}.sum)
     val a2Fenmu = Math.sqrt(a2.map{num=>num*num}.sum)
     
     val cos = fenzi/(a1Fenmu*a2Fenmu)
     cos
   }
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
}