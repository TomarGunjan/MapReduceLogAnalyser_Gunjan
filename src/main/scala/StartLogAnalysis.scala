
import com.typesafe.config.ConfigFactory
import combiners.Combiner
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import org.apache.log4j.Logger
import org.apache.log4j.spi.LoggerFactory
import mappers.*
import org.apache.commons.io.FileUtils.copyFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.lib.{CombineFileInputFormat, CombineTextInputFormat}
import reducers.*

import java.io.File

object StartLogAnalysis:
  /*
  This is the entry point for running log analyser Map Reducer Jobs.
  the runMapReduce takes 3 arguments
  A. jobId - Integer type argument.Using this value a job is picked. Based on its value following job can be picked
      1 - Job 1 Time Interval Distribution of logs with matching pattern
      2 - Job 2 Error type log distribution with message matching pattern
      3 - Job 3 Counting number of message for each log type
      4 - Job 4 Counting character for log message matching pattern
      5 - all jobs will be run
      any other input - no action
*/
  //creating logger
  val logger: Logger = Logger.getLogger(getClass.getName)
  //loading config
  val config = ConfigFactory.load()
  //local strings to be user
  val local = "local"
  val outputFileName = "/output.csv"
  val pathExtension = "/part-00000"
  val job1 = "1"
  val job2 = "2"
  val job3 = "3"
  val job4 = "4"
  val allJobs = "5"

  @main def runMapReduce(jobId: String, inputPath: String, outputPath: String): Unit =

   // running different job based on user input

    if (jobId == job1||jobId==allJobs) {
      runJob1(inputPath, outputPath)
    }

    if (jobId == job2||jobId==allJobs)  {
      runJob2(inputPath,outputPath)
    }

    if (jobId == job3||jobId==allJobs) {
      runJob3(inputPath, outputPath)
    }

    if (jobId == job4||jobId==allJobs) {
      runJob4(inputPath, outputPath)
    }

  def runJob1(inputPath: String, outputPath: String):Unit =
    //Initialising Job configurations such as assigning mapper and reducers, setting input/output format
    logger.info("Job 1 was picked to compute distribution of different messages type for predefined time intervals matching provided pattern")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("LogAnalyserJob1")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    conf.setMapperClass(classOf[TimeDistributionMapper])
    conf.setReducerClass(classOf[TimeIntDistributionReducer])
    val timestamp = (System.currentTimeMillis / 1000).toString
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //output will be stored in afile with name outputfoldername_job_{{job_id}}
    val opPath = outputPath.concat("_Job").concat("_1_").concat(timestamp)
    FileOutputFormat.setOutputPath(conf, new Path(opPath))
    logger.info("Job triggered")
    JobClient.runJob(conf)
    if(config.getString("LogMessageInfo.env")==local) {
      new File(opPath.concat(pathExtension)).renameTo(new File(opPath.concat(outputFileName)))
    }

  def runJob2(inputPath: String, outputPath: String):Unit =
    //Initialising Job configurations such as assigning mapper and reducers, setting input/output format
    logger.info("Job 2 was picked to compute distribution of ERROR messages type for interval chunks of provided time interval in minutes where message matches provided pattern")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("LogAnalyserJob2Part1")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    conf.setMapperClass(classOf[ErrorMsgDistributionMapper])
    conf.setReducerClass(classOf[ErrorMsgDistributionReducer])
    val timestamp = (System.currentTimeMillis / 1000).toString
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //output will be stored in afile with name outputfoldername_job_{{job_id}}
    val opPath = outputPath.concat("_Job").concat("_2_1_").concat(timestamp)
    FileOutputFormat.setOutputPath(conf, new Path(opPath))
    logger.info("Job 2 Part 1 triggered")
    JobClient.runJob(conf)

    //pipelining output from first job to a second job
    logger.info("Triggering part 2 of job 2")
    val conf2: JobConf = new JobConf(this.getClass)
    conf2.setJobName("LogAnalyserJob2Part2")
    //conf.set("fs.defaultFS", "local")
    conf2.set("mapreduce.job.maps", "1")
    conf2.set("mapreduce.job.reduces", "1")
    conf2.set("mapreduce.output.textoutputformat.separator", ",")
    conf2.setOutputKeyClass(classOf[Text])
    conf2.setOutputValueClass(classOf[IntWritable])
    conf2.setInputFormat(classOf[TextInputFormat])
    conf2.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    conf2.setMapperClass(classOf[SortMapper])
    conf2.setMapOutputKeyClass(classOf[IntWritable])
    conf2.setMapOutputValueClass(classOf[Text])
    conf2.setReducerClass(classOf[SortReducer])
    FileInputFormat.setInputPaths(conf2, new Path(opPath.concat(pathExtension)))
    val opPath2 = outputPath.concat("_Job").concat("_2_2_").concat(timestamp)
    FileOutputFormat.setOutputPath(conf2, new Path(opPath2))
    logger.info("Job 2 Part 2 triggered")
    JobClient.runJob(conf2)
    if (config.getString("LogMessageInfo.env") == local) {
      new File(opPath2.concat(pathExtension)).renameTo(new File(opPath2.concat(outputFileName)))
    }


  def runJob3(inputPath: String, outputPath: String):Unit =
    //Initialising Job configurations such as assigning mapper and reducers, setting input/output format
    logger.info("Job 3 was picked to compute count of different messages type")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("LogAnalyser")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    conf.setMapperClass(classOf[MessageCountMapper])
    conf.setReducerClass(classOf[MessageCountReducer])
    val timestamp = (System.currentTimeMillis / 1000).toString
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    val opPath = outputPath.concat("_Job").concat("_3_").concat(timestamp)
    FileOutputFormat.setOutputPath(conf, new Path(opPath))
    logger.info("Job triggered")
    JobClient.runJob(conf)
    if (config.getString("LogMessageInfo.env") == "local") {
      new File(opPath.concat("\\part-00000")).renameTo(new File(opPath.concat("\\output.csv")))
    }


  def runJob4(inputPath: String, outputPath: String):Unit =
    //Initialising Job configurations such as assigning mapper and reducers, setting input/output format
    logger.info("Job 4 was picked to compute character count for all messages matching provided pattern")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("LogAnalyser")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    conf.setMapperClass(classOf[MaxCharacterCountMapper])
    conf.setReducerClass(classOf[MaxCharacterCountReducer])
    val timestamp = (System.currentTimeMillis / 1000).toString
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    val opPath = outputPath.concat("_Job").concat("_4_").concat(timestamp)
    FileOutputFormat.setOutputPath(conf, new Path(opPath))
    logger.info("Job triggered")
    JobClient.runJob(conf)
    if (config.getString("LogMessageInfo.env") == local) {
      new File(opPath.concat(pathExtension)).renameTo(new File(opPath.concat(outputFileName)))
    }