# MapReduceLogAnalyser_Gunjan
### Repository to solve Diffusion computation using Map Reduce Framework
Submitted By - Gunjan Tomar

UIN - 674659382

Email Id : gtomar2@uic.edu

## Overview
This objective of this repository is to analyse log files and produce perform following computations in different jobs and produce a csv

Job 1 - The distribution of different types of messages(matching a certain predefined regex pattern) across predefined time intervals.

Job 2 - The time intervals that contained most log messages of the type ERROR with injected regex pattern string instances. 

Job 3 - The number of the generated log messages for each message type(ERROR,DEBUG,INFO,WARN 

Job 4 - The number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern

## About Framework
Hadoop MapReduce is a software framework for easily writing applications which process vast amounts of data (multi-terabyte data-sets) in-parallel on large clusters (thousands of nodes) of commodity hardware in a reliable, fault-tolerant manner.
The framework consists of mappers and reducers. The input data is divided in chunks and given to multiple mapreduces to be processed parallely. 

## Project Details

![fw](https://user-images.githubusercontent.com/26132783/194685394-58838fa4-fe14-40f5-8404-f68bc4fb96af.png)

 

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
- Create a directory called input 

    
  
