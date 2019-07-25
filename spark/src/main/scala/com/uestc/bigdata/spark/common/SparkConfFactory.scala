package com.uestc.bigdata.spark.common


import org.apache.spark.{Logging, SparkConf}

/**
  * 创建sparkconf
  */
object SparkConfFactory extends Serializable with Logging{

    private val SPARK_START_CONFIG="/spark/spark-start-config.properties"
    private val SPARK_STREAMING_CONFIG="/spark/spark-streaming-config.properties"

  /**
    * 创建本地spark conf环境
    * @param appName
    * @param thread
    */
    def newSparkLocalConf(appName:String="Spark Local",thread: Int=1):SparkConf={
    new SparkConf().setAppName(appName).setMaster(s"local[$thread]")
  }

  /**
    * 创建集群模式环境
    * @param appName
    * @return
    */
  def newSparkConf(appName:String="Spark Cluster"): SparkConf ={
    new SparkConf().setAppName(appName)
  }
}
