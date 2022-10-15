package reducers

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import org.apache.log4j.Logger

import java.util
import scala.jdk.CollectionConverters.*

/* Job 2 Reducer 1
    count messages found in same interval and outputs their count against the interval
*/
class ErrorMsgDistributionReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)
  
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 2 Reducer has been triggered")
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key, new IntWritable(sum.get()))
