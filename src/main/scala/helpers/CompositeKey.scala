package helpers

import org.apache.hadoop.io.{IntWritable, WritableComparable}
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException


class CompositeKey(key:Int) extends WritableComparable[CompositeKey] :



  def compareTo(input:CompositeKey):Int = {
    input.getKey().compareTo(this.key)
  }



  @throws[IOException]
  def write(out: DataOutput): Unit = {
    out.writeInt(key)
  }

  @throws[IOException]
  def readFields(in: DataInput): Unit = {
  }
  
  def getKey():Int= {
    key
  }
