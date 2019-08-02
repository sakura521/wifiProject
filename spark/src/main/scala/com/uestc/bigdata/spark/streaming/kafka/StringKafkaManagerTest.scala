package com.uestc.bigdata.spark.streaming.kafka

import java.util.Properties

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.spark.common.SparkContextFactory
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager}


/**
  * [chl_test8,2]
  * [chl_test8,1]
  * [chl_test8,0]
  * ([chl_test8,2],1306)
  * ([chl_test8,1],1409)
  * ([chl_test8,0],1389)
  * kafka过期策略清楚了前面的RDD信息
  * kafka最早的offset:Map([chl_test8,2] -> LeaderOffset(linux4,9092,1306), [chl_test8,1] -> LeaderOffset(linux6,9092,1409), [chl_test8,0] -> LeaderOffset(linux5,9092,1389))
  * 更新的offset=====================Map([chl_test8,2] -> 1306, [chl_test8,1] -> 1409, [chl_test8,0] -> 1389)
  */
object StringKafkaManagerTest extends Serializable with Logging{
  private val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics="chl_test8"
  def main(args: Array[String]): Unit = {

  //  val kafkaParams = KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"),"xiaofei")
    val kafkaParams = getKafkaParam(topics,"ggg1")
    val ssc = SparkContextFactory.SparkStreamingLocal("StringKafkaManagerTest",
      20L,
      1)

    val kafkaDS = new KafkaManager(kafkaParams, true)
      .createJsonToMapStringDirectSteramWithOffset(ssc, Set(topics))
      .persist(StorageLevel.MEMORY_AND_DISK)

      kafkaDS.foreachRDD(rdd=>{
        rdd.foreach(println(_))
    })
    ssc.start()
    ssc.awaitTermination()
  }



  def getKafkaParam(kafkaTopic:String,groupId:String): Map[String,String]={
    val kafkaParam = Map[String,String](
      "metadata.broker.list" -> "linux4:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId,
      "refresh.leader.backoff.ms" -> "1000",
      "num.consumer.fetchers" -> "8")
    kafkaParam
  }
}
