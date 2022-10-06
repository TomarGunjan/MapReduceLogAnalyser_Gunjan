package combiners

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}
import org.apache.log4j.Logger
import java.util
import scala.jdk.CollectionConverters.*

class GeneralCombiner extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :

  private val logger: Logger = Logger.getLogger(getClass.getName)

  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    logger.info("Combiner is triggered")
    values.asScala.foreach(value => output.collect(key, value))


