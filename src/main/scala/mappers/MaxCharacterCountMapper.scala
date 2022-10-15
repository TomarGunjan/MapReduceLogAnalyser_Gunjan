package mappers

import com.typesafe.config.ConfigFactory
import helpers.HelperUtils
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.log4j.Logger

import java.io.IOException
import java.time.LocalTime
import java.time.temporal.ChronoField

/*Job 4 Mapper : Counting character for log message matching pattern
    this mapper takes value from a file line by line 
    for each log value with msg is checked for provided pattern
    if pattern matches the mathed message along with its length and message type is collected
*/

class MaxCharacterCountMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)
  
  @throws[IOException]
  def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 4 mapper started")
    val tuple = HelperUtils().maxCharacterHelper(value)
    if(tuple._1!=null){
      logger.info(s"a match was found and key ${tuple._1} and value ${tuple._2} will be stored")
      output.collect(tuple._1, tuple._2)
    }


