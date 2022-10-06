package reducers

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import org.apache.log4j.Logger

import java.util
import scala.jdk.CollectionConverters.*

/*
  Job 3 Reducer
  counts messages for each type and output number of messages for each type
*/

class MessageCountReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)

  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 4 Reducer is triggered")
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key, new IntWritable(sum.get()))
