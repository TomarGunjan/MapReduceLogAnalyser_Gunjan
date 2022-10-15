# MapReduceLogAnalyser_Gunjan
### Repository to solve Diffusion computation using Map Reduce Framework
Submitted By - Gunjan Tomar

UIN - 674659382

Email Id : gtomar2@uic.edu

## Overview
This objective of this repository is to analyse log files and produce perform following computations in different jobs and produce a csv

Job 1 - The distribution of different types of messages(matching a certain predefined regex pattern) across predefined time intervals.

Job 2 - The time intervals sorted in descending order of that contained most log messages of the type ERROR with injected regex pattern string instances. 

Job 3 - The number of the generated log messages for each message type(ERROR,DEBUG,INFO,WARN 

Job 4 - The number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern

## About Framework
Hadoop MapReduce is a software framework for easily writing applications which process vast amounts of data in-parallel on large clusters (thousands of nodes) of commodity hardware in a reliable, fault-tolerant manner.

The framework consists of mappers and reducers. The input data is divided in chunks and given to multiple mapreduces to be processed parallely.

There are 2 stages of data processing in MapReduce Framework

Mapper - Here the mapper takes input in format of key-value pair and outputs data in key value pair upon some processing <k1,v1>

Reducer - Here the reducer takes the output from mapper in the form of key and all values mapped against it and generated output in form of key value pair <k2,v2>

For example say a file contains strings and we want to extract all words and their occurence. 

Mapper input - the file

Mapper output - The key would be each word and value would be 1 for each occurence

Reducer input - The key would be the key from mapper output and value would be a list of values which has same key in this case a list of 1

Reducer output - After iterating over values from the input, the total number would be calculated and reducer out put would have word as key and its count as value

## Project Details and some things to note

The project structure can be summarised as below

![image](https://user-images.githubusercontent.com/26132783/196000767-7690fff2-6465-4b31-a710-489da76ea4e2.png)

 
The main function in StartLogAnalyser.scala class takes 3 arguments
 - jobId
 - input path
 - output path
 
The "***env***" config value should be set to "local" to have output files generated as .csv files

Job 1, 3 and 4 consists of single mapper and Reducer class whereas in Job 2 The put put of reducer has been pipelined to another job with a different mapper and Reducer class

## How to run?

### Pre Requisites
- Simple Build Toolkit (SBT)
- Apache Hadoop

### Create Jar
- Clone the project
- Open sbt terminal and navigate to project (if not already there)
- Run following command "sbt clean compile assembly"
- A jar should be created inside target folder

### Run on windows machine
- Create jar using above command
- Open cmd and start the nodes using start-dfs.cmd and start-yarn.cmd
- Create input directory and move the input file to the directory using dfs
  - hdfs dfs -mkdir input
  - hdfs dfs -put <path to log file> /input
- run the created jar with following account and pass space separated input parameters as mentioned in Project Details section
  hadoop jar <path to jar> <input parameters>

 
### Run on AWS EMR
- Create a new bucket in AWS EMR with default configs
- Upload the create jar to the bucket
- Create a folder with name input in the bucket and upload the input file inside the the folder
- Create a cluster with default config(select emr release 6 for hadoop 3.x.y)
- Add step to the cluster, select custom jar and provide s3 path for the uploaded jar file
- provide input arguments with job id and s3 path to input folder and output folder name

## Output Format

### 1. Job 1 Sample
The output shows total number of messages matched with a pattern(provided in config) in a time interval(start time and end time in config). Column 1 shows Message Type and column 2 shows the count of messages matched

![image](https://user-images.githubusercontent.com/26132783/196001161-0121a9d2-3ea3-4fe0-be83-84887011f1f9.png)

### 2. Job 2 Sample
The output shows the error messages distribution in the log file. Column 1 shows the time interval and column 2 shows number of error messages matched with the pattern(provided in config). The out put is arranged in descending order of message count

![image](https://user-images.githubusercontent.com/26132783/196001367-d7be231c-75d7-4fea-9717-aa2572cc207a.png)

### 3. Job 3 Sample
The output shows number of messages matched with pattern(provided in config) in the entire input file. Column 1 shows message type and column 2 shows the counts of messages matched

![image](https://user-images.githubusercontent.com/26132783/196001525-bfcbd584-bcf2-4ae9-bb1a-c797d89454d4.png)

### 4. Job 4 Sample
The output shows the longest string matched for each message type. Column 1 shows message type, column 2 shows length of the longest string matched for that message type and column 3 shows the matched string itself

![image](https://user-images.githubusercontent.com/26132783/196002244-fe01781e-42a3-4f7a-9205-34d4f7fa0353.png)



## Sources
 1. Dr. Grechanik, Mark, (2020) Cloud Computing: Theory and Practice.
 2. [Apache Hadoop](https://hadoop.apache.org/)
 3. [AWS](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html)

    
  
