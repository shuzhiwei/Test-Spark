package cn.tedu.ssort

class Ssort(val col1:String, val col2:Int) extends Ordered[Ssort] with Serializable{
  //
  def compare(that:Ssort):Int={
    //按第一列升序
    val result=this.col1.compareTo(that.col1)
    if(result==0){
      //按第二列降序
      that.col2.compareTo(this.col2)
    }else{
      result
    }
  }
}