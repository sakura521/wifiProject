package com.uestc.bigdata.spark.common

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkcontext执行环境入口
  * streamingContext
  */
object SparkContextFactory {

  def SparkStreamingLocal(appName:String="sparkStreaming",batchINrerval:Long=40L,thread:Int=1): StreamingContext = {
    val sparkConf = SparkConfFactory.newSparkLocalConf(appName, thread)
    //设置kafka拿取的数据量
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    new StreamingContext(sparkConf, Seconds(batchINrerval))
  }

  def SparkStreaming(appName:String="Spark stream",batchINreval:Long=40L): StreamingContext ={
    val sparkConf = SparkConfFactory.newSparkConf(appName)
    new StreamingContext(sparkConf,Seconds(batchINreval))
  }
}
