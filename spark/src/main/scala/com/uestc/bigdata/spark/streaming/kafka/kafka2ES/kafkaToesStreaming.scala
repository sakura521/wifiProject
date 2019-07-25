package com.uestc.bigdata.spark.streaming.kafka.kafka2ES

import java.util
import java.util.Properties

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.common.dataType.DataTypeUtils
import com.uestc.bigdata.spark.common.SparkContextFactory
import com.uestc.bigdata.spark.streaming.kafka.KafkaParamerUtil
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._
/**
  * kafka数据流全量入es；按照table的种类存放
  */
object kafkaToesStreaming extends Serializable with Logging{
  // 1. 获取tables信息
  private val tables: util.Set[String] = DataTypeUtils.dataType.keySet()

  //2.获取kafkaconfig信息
  private val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")

  def main(args: Array[String]): Unit = {
    //3. 定义kafka主题
    val  topics=args(0).split(",")

    // 4. 创建 streamingContext环境入口
    val ssc = SparkContextFactory.SparkStreamingLocal("kafkaToesStreaming",
      30L, 1)

    // 5.创建数据流
    val kafkaDS = new KafkaManager(KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"), "consumer1"), true)
      .createJsonToMapStringDirectSteramWithOffset(ssc, topics.toSet)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 5.过滤每个表的内容 对每条数据进行过滤插入
      tables.foreach(table=>{
        val data = kafkaDS.filter(x=>{table.equals(x.get("table"))})
         KafkaToEsJob.insertToEs(table,data)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
