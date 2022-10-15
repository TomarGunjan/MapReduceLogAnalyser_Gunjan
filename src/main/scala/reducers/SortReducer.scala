package reducers

import com.typesafe.config.ConfigFactory
import helpers.CompositeKey
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import org.apache.log4j.Logger

import java.util
import scala.jdk.CollectionConverters.*

/* Job 2 Reducer 2
    takes key as negation of count of messages and value as list of intervals interval from the mapper 
    it splits the intervals in individual outputs and outputs each interval with corresponding value. 
    The negation from the key is removed
*/

class SortReducer extends MapReduceBase with Reducer[IntWritable, Text, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)

  override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 2 Reducer has been triggered")
    values.asScala.foreach(value =>
      value.toString.split(",").foreach(loc =>
        output.collect(new Text(loc), new IntWritable(-key.get()))
      )
    )
