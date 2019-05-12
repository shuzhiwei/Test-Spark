package cn.tedu.test

class MySsort(val clo1:String, val clo2:Int) extends Ordered[MySsort] with Serializable{
  def compare(that:MySsort)={
    val result = this.clo1.compareTo(that.clo1)
    if(result == 0){
      that.clo2.compareTo(this.clo2)
    }else{
      result
    }
  }
}