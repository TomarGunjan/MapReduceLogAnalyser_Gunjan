package mappers

import com.typesafe.config.ConfigFactory
import helpers.HelperUtils
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.hadoop.thirdparty.org.checkerframework.checker.regex.qual.Regex
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.LocalTime
import java.util

/*Job 1 Mappr : Time Interval Distribution of logs with matching pattern
    this mapper takes value from a file line by line 
    for each log value the timestamp is checked if it falls under provided time interval
    if it falls under predefined time interval then message is checked for provided pattern
    if pattern matches the message details are collected with value 1
    time interval duration is decided based on values provided in configuration.conf file
*/

class TimeDistributionMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

  //Creating Logger
  private val logger : Logger = Logger.getLogger(getClass.getName)

  //Creating IntWritable(Hadoop flavor of Integer) object with value 1
  private final val one = new IntWritable(1)
  

//Creating Mapper Function
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("Job 4 mapper started")
      val key = HelperUtils().timeDistributionHelper(value)
      if (key != null){
        logger.info(s"a match was found and key ${key} and value $one will be stored")
        output.collect(key, one)
      }
      

      




