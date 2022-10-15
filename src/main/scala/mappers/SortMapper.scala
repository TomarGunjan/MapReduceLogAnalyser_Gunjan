package mappers


import com.typesafe.config.ConfigFactory
import helpers.{CompositeKey, HelperUtils}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.log4j.Logger

import java.io.IOException
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.StringTokenizer
import scala.collection.immutable.ListMap
import helpers.CompositeKey

/*Job 2 Mappr 2 : Error type log distribution with message matching pattern
    this mapper takes out put of Reducer 1 output as input
    the count of messages found is negated and set as key and the time interval is set as value
    The negation is introduced in key to sort the keys in descending order of their magnitude
*/

class SortMapper extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :

  //Creating Logger
  private val logger: Logger = Logger.getLogger(getClass.getName)

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
    logger.info("Job2 mapper 2 started")
    val splits = value.toString.split(",")
    val nKey = -splits(1).toInt
    val nVal = splits(0)
    val k= new IntWritable(nKey)
    output.collect(k,new Text(nVal))




