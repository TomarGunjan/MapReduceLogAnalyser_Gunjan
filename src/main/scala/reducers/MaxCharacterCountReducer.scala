package reducers

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import org.apache.log4j.Logger

import java.util
import scala.jdk.CollectionConverters.*

/*
  Job 4 Reducer
  takes character count for matched messages and passes them to output
*/

class MaxCharacterCountReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)
  
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 4 Reducer is triggered")
    values.asScala.foreach(value=>output.collect(key, value))