import com.mifmif.common.regex.Generex
import com.typesafe.config.ConfigFactory
import helpers.HelperUtils
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.deprecated.symbolLiterals

class MRLogTest extends AnyFlatSpec with Matchers{
  behavior of "HelperUtils"
  val INPUTSTRING = "17:50:01.284 [scala-execution-context-global-17] ERROR  HelperUtils.Parameters$ - s%af0ce3H6sA5hM8qR7hae3bf1H8jV8jz"
  val INPUTSTRINGWITHNOMATCH = "17:50:01.284 [scala-execution-context-global-17] ERROR  HelperUtils.Parameters$ - s%af-ce3H=sA5hM8.R7ha'3bf1,8jV.jz"
  val input = Text(INPUTSTRING)
  val inputWithNoMatch = Text(INPUTSTRINGWITHNOMATCH)
  val helperClass = HelperUtils()
  val config = ConfigFactory.load()
  val timePattern = config.getString("LogMessageInfo.timePattern").r

  it should "filter out data for matching patterns for each msg type for time intervals present in config" +
    "for matching pattern" in {
    val expectedOutput = "ERROR"
    val output = helperClass.timeDistributionHelper(input)
    output.toString should include (expectedOutput)
  }

  it should "not return any data if there was no match for the pattern" in {
    val output = helperClass.timeDistributionHelper(inputWithNoMatch)
    output shouldBe(null)
  }

  it should "filter out data against time interval specified in configuration and give count of entries found " +
    "for matching pattern" in {
    val output = helperClass.errMsgDistributionHelper(input)
    output should not equal(null)
  }

  it should "not produce result if there was no match" in {
    val output = helperClass.errMsgDistributionHelper(inputWithNoMatch)
    output shouldBe(null)
  }

  it should "return msg type" in {
    val expectedOutput = new Text("ERROR")
    val output = helperClass.msgCounterHelper(input)
    output.shouldBe(expectedOutput)
  }

  it should "return matched pattern and its length from the given input" in {
    val expectedOutput = new Text("ERROR,af0ce3H6sA5hM8qR7hae3bf1H8jV8j")
    val output = helperClass.maxCharacterHelper(input)
    output._1.shouldBe(expectedOutput)
  }

  it should "return no output if there was no match" in {
    val output = helperClass.maxCharacterHelper(inputWithNoMatch)
    output.shouldBe(Tuple2(null,null))
  }

}
