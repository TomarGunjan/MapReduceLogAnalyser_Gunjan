package mappers

import com.typesafe.config.ConfigFactory
import helpers.HelperUtils
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.log4j.Logger

import java.io.IOException
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

/*Job 2 Mappr : Error type log distribution with message matching pattern
    this mapper takes value from a file line by line 
    for each log value with msg type as error the message is checked for provided pattern
    if pattern matches the message details are stored against the time interval
    time interval duration is decided based on value present in config and 24 hours are divided in intervals of
     that duration
*/

class ErrorMsgDistributionMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)

  //Creating IntWritable(Hadoop flavor of Integer) object with value 1
  private final val one = new IntWritable(1)
  

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Job2 mapper has been triggered")
    val key = HelperUtils().errMsgDistributionHelper(value)
    if(key!=null) {
      logger.info(s"a match was found and key $key  and value $one will be stored")
      output.collect(key, one)
    }


