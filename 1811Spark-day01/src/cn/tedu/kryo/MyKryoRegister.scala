package cn.tedu.kryo

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

class MyKryoRegister extends KryoRegistrator{
  def registerClasses(kryo:Kryo):Unit={
    
  }
}