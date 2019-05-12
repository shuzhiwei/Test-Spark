package cn.tedu.movie

object Driver {
  def main(args: Array[String]): Unit = {
    //--计算出user1与user2的相似度
    val user1FilmSource=Map("m1"->2,"m2"->3,"m3"->1,"m4"->0,"m5"->1)
    
    val user2FilmSource=Map("m1"->1,"m2"->2,"m3"->2,"m4"->1,"m5"->4)
    
    val user3FilmSource=Map("m1"->2,"m2"->1,"m3"->0,"m4"->1,"m5"->4)
    
    val user4FilmSource=Map("m1"->3,"m2"->2,"m3"->0,"m4"->5,"m5"->3)
  
    val user5FilmSource=Map("m1"->5,"m2"->3,"m3"->1,"m4"->1,"m5"->2)
    
    println(cosMap(user1FilmSource, user2FilmSource))
    println(cosMap(user1FilmSource, user3FilmSource))
    println(cosMap(user1FilmSource, user4FilmSource))
    println(cosMap(user1FilmSource, user5FilmSource))
   
    
  }
  def cosMap(m1:Map[String,Int],m2:Map[String,Int])={
    val m1m2 = m1 zip m2
    val m1m2Fenzi = m1m2.map{x=>x._1._2*x._2._2}.sum
    val m1Fenmu = Math.sqrt(m1.map{x=>x._2*x._2}.sum)
    val m2Fenmu = Math.sqrt(m2.map{x=>x._2*x._2}.sum)
    val m1m2cos = m1m2Fenzi/(m1Fenmu*m2Fenmu)
    m1m2cos
    
  }
  
  
}