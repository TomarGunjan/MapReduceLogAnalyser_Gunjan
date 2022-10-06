import combiners.GeneralCombiner
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import org.apache.log4j.Logger
import org.apache.log4j.spi.LoggerFactory
import mappers.*
import org.apache.hadoop.fs.Path
import reducers.*
import combiners.*
import java.io.File

object StartLogAnalysis:
  /*
  This is the entry point for running log analyser Map Reducer Jobs.
  the runMapReduce takes 3 arguments
  A. jobId - Integer type argument.Using this value a job is picked. Based on its value following job can be picked
      1 - Job 1 Time Interval Distribution of logs with matching pattern
      2 - Job 2 Error type log distribution with message matching pattern
      3 - Job 3 Counting number of message for each log type
      any other value - Job 4 Counting character for log message matching pattern
*/
  @main def runMapReduce(jobId: String, inputPath: String, outputPath: String): Unit =

    //creating logger
    val logger: Logger = Logger.getLogger(getClass.getName)

    //Initialising Job configurations such as assigning mapper and reducers, setting input/output format
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "2")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    logger.info("Starting Map Reducer Job")

    //mapper and reducer class will be set based on job picked
    if (jobId == "1") {
      logger.info("Job 1 was picked to compute distribution of different messages type for predefined time intervals matching provided pattern")
      conf.setMapperClass(classOf[TimeDistributionMapper])
      conf.setReducerClass(classOf[TimeIntDistributionReducer])
    } else if (jobId == "2") {
      logger.info("Job 2 was picked to compute distribution of ERROR messages type for interval chunks of provided time interval in minutes where message matches provided pattern")
      conf.setMapperClass(classOf[ErrorMsgDistributionMapper])
      conf.setCombinerClass(classOf[ErrorMsgDistributionReducer])
      conf.setReducerClass(classOf[ErrorMsgDistributionReducer])
    } else if (jobId == "3") {
      logger.info("Job 3 was picked to compute count of different messages type")
      conf.setMapperClass(classOf[MessageCountMapper])
      conf.setCombinerClass(classOf[MessageCountReducer])
      conf.setReducerClass(classOf[MessageCountReducer])
    } else {
      logger.info("Job 4 was picked to compute character count for all messages matching provided pattern")
      conf.setMapperClass(classOf[MaxCharacterCountMapper])
      conf.setCombinerClass(classOf[MaxCharacterCountReducer])
      conf.setReducerClass(classOf[MaxCharacterCountReducer])
    }
    //for combining results from multiple mappers
    conf.setCombinerClass(classOf[GeneralCombiner]);
    val timestamp = (System.currentTimeMillis / 1000).toString
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //output will be stored in afile with name outputfoldername_job_{{job_id}}
    val opPath = outputPath.concat("_Job").concat(jobId).concat("_").concat(jobId).concat(timestamp)
    FileOutputFormat.setOutputPath(conf, new Path(opPath))
    logger.info("Job triggered")
    JobClient.runJob(conf)
    new File(opPath.concat("\\part-00000")).renameTo(new File(opPath.concat("\\output.csv")))



