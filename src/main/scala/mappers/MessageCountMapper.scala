package mappers

import com.typesafe.config.ConfigFactory
import helpers.HelperUtils
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.log4j.Logger

import java.io.IOException
import java.time.LocalTime

/*Job 2 Mapper : Error type log distribution with message matching pattern
    this mapper takes value from a file line by line 
    for each log value its msg type with value 1 is collected
*/

class MessageCountMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)
  
  //Creating IntWritable(Hadoop flavor of Integer) object with value 1
  private final val one = new IntWritable(1)

  //Creating Mapper Function
  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job 3 mapper started")
    val key = HelperUtils().msgCounterHelper(value)
    if (key != null){
      logger.info(s"a match was found and key ${key} and value $one will be stored")
      output.collect(key, one)
    }

