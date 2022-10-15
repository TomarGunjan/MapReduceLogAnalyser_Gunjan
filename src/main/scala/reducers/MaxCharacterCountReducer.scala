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

class MaxCharacterCountReducer extends MapReduceBase with Reducer[Text, Text, Text, Text] :

  //Creating Logger
 
    private val logger: Logger = Logger.getLogger(getClass.getName)

    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      logger.info("Job 4 Reducer is triggered")
      //need this variable as need to extract max value of logs for a message type using comparison for each value against key
      var message=""

      values.asScala.foreach(value =>
        if(value.getLength>message.length){
          message=value.toString
        }
      )
      output.collect(key, Text(message.length.toString.concat(",").concat(message)))