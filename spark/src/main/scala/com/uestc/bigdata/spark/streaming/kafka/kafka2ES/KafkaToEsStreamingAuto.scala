package com.uestc.bigdata.spark.streaming.kafka.kafka2ES

import java.util
import java.util.Properties

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.common.dataType.DataTypeUtils
import com.uestc.bigdata.spark.common.SparkContextFactory
import com.uestc.bigdata.spark.streaming.kafka.KafkaParamerUtil
import com.uestc.bigdata.common.time.TimeTransformUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._
object  KafkaToEsStreamingAuto extends Serializable with Logging{
  // 1. 获取tables信息
  private val tables: util.Set[String] = DataTypeUtils.dataType.keySet()

  //2.获取kafkaconfig信息
  private val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")

  def main(args: Array[String]): Unit = {
    //3. 定义kafka主题
    val topics="chl_test0"
   // val  topics=args(0).split(",")

    // 4. 创建 streamingContext环境入口
    val ssc = SparkContextFactory.SparkStreamingLocal("KafkaToEsStreamingAuto",
      30L, 1)

    // 5.创建数据流
    val kafkaDS = new KafkaManager(KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"), "consumer-1"), true)
      .createJsonToMapStringDirectSteramWithOffset(ssc, Set(topics))
      .map(x=>{
      x.put("index_date",TimeTransformUtils.Date2yyyyMMdd(x.get("collect_time")))
        x
      })
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 5.过滤每个表的内容 对每条数据进行过滤插入
    tables.foreach(table=>{
      val data = kafkaDS.filter(x=>{table.equals(x.get("table"))})
      KafkaToEsJob.insertIntoESbyDate(table,data)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
