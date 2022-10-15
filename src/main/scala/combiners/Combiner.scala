package combiners

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import java.util
import scala.jdk.CollectionConverters.*

class Combiner extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
  
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    //logger.info("Job 4 Reducer is triggered")
    values.asScala.foreach(value => output.collect(key, value))

