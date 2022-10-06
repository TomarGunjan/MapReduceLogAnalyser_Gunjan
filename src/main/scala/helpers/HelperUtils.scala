package helpers

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.log4j.Logger

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

/*
  This class provides helper function for mappers. Each function in this class takes Text type argument
*/
class HelperUtils {
  

  //Loading config
  private val config = ConfigFactory.load()

  //Getting common config values to be used in mapper functions
  private val pattern = config.getString("LogMessageInfo.pattern").r
  private val timePattern = config.getString("LogMessageInfo.timePattern").r
  private val timeInterval = config.getInt("LogMessageInfo.timeInterval")
  val msgTypePattern = config.getString("LogMessageInfo.msgTypePattern").r


  //helper function for Job 1 checks if given contains regez of given pattern and if it falls under predefined time.
  //returns msg type if true
  def timeDistributionHelper(value: Text): Text =
  
    //start and end time of predefined interval to check messages in  
    val startTime: LocalTime = LocalTime.parse(config.getString("LogMessageInfo.startTime"))
    val endTime: LocalTime = LocalTime.parse(config.getString("LogMessageInfo.endTime"))

    val line: String = value.toString
    val timeMatch = timePattern.findFirstIn(line)
    timeMatch match {
      case Some(s) => {
        val inputTime = LocalTime.parse(s)
        //checking if given log timestamp falls between start and end time
        if ((inputTime.equals(startTime) || inputTime.isAfter(startTime)) && (inputTime.equals(endTime) || inputTime.isBefore(endTime))) {
          val msgMatch = pattern.findFirstIn(line)
          val msgMatchType = msgTypePattern.findFirstIn(line)
          //cheking if log contains message pattern
          msgMatch match {
            case Some(msg) => {
              msgMatchType match {
                case Some(msgType) => {
                  //returning msg type
                  new Text(msgType)
                }
                case None =>
                   null
              }
            }
            case None =>
               null
          }
        } else
           null
      }
      case None =>
         null
    }

  //helper function for Job 2 checks if given log contains ERROR and given regex pattern
  //if true time interval based on predefined duration is fixed and returned with message data
  def errMsgDistributionHelper(value: Text): Text =
    // variable to manipulate and generate from and to value for a time interval in localdatetime format
    val resetTime = LocalTime.of(0, 0, 0, 0)
    //date time formatter to format time in specified format
    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.nnn")
    //msgpattern to match "ERROR"
    val errMsgPattern = config.getString("LogMessageInfo.errorMsgPattern").r
    
    val line: String = value.toString
    val msgType = errMsgPattern.findFirstIn(line)
    msgType match {
      //checking in log is of type "ERROR"
      case Some(msg) => {
        val matchMsg = pattern.findFirstIn(line)
        //checking is log has a match for regex pattern
        matchMsg match {
          case Some(m)=>{
            //fining timestamp for log
            val timeMatch = timePattern.findFirstIn(line)
            timeMatch match {
              case Some(t) => {
                //calculating time interval
                val time = LocalTime.parse(t)
                val second = time.get(ChronoField.SECOND_OF_MINUTE)
                val nano = time.get(ChronoField.NANO_OF_SECOND)
                val minutes = if (second > 0 || nano > 0) time.get(ChronoField.MINUTE_OF_DAY) + 1 else time.get(ChronoField.MINUTE_OF_DAY)
                val endMinutes = Math.round(minutes / timeInterval) * timeInterval
                val from = resetTime.plusMinutes(endMinutes - timeInterval).plusNanos(1).format(formatter)
                val to = resetTime.plusMinutes(endMinutes).format(formatter)
                val key = from.concat("-").concat(to)
                new Text(key)
              }
              case None => {
                 null
              }
            }
          }
          case None=>{
             null
          }
        }
      }
      case None => {
        return null
      }
    }

  //helper function for Job 3 checks the msg type and returns Text type object with msg type as value
  def msgCounterHelper(value: Text): Text = {
    val line: String = value.toString
    val msgMatchType = msgTypePattern.findFirstIn(line)
    //checking log message type
    msgMatchType match {
      case Some(msgType) => {
        //returning message type
         new Text(msgType)
      }
      case None => {
         null
      }
    }
  }

  //helper function for Job 4 checks if given line matches value for given regex 
  //if true returns msg type and matched msg along with its length
  def maxCharacterHelper(value:Text) : (Text, IntWritable) = {
    val line: String = value.toString
    val msgMatch = pattern.findFirstIn(line)
    //checking if log message matches provided pattern
    msgMatch match
      case Some(msg) => {
        val msgType = msgTypePattern.findFirstIn(line)
        //checking type of log
        msgType match {
          case Some(mt) => {
            val key = mt.concat(",").concat(msg)
            //returning msgType,matchedMessage as key and length of matched message as value
            (Text(key), IntWritable(msg.length))
          }
          case None =>
            (null, null)
        }
      }
      case None => {
        (null, null)
      }
  }
}



