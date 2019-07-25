package com.uestc.bigdata.spark.streaming.kafka

import com.uestc.bigdata.spark.common.{SparkConfFactory, SparkContextFactory}
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, KafkaUtils}
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.streaming.dstream.InputDStream


/**
  * 获取RDD中的偏移信息OffsetRange(topic: 'chl_test8', partition: 2, range: [0 -> 30])
  * 获取RDD中的偏移信息OffsetRange(topic: 'chl_test8', partition: 1, range: [0 -> 30])
  * 获取RDD中的偏移信息OffsetRange(topic: 'chl_test8', partition: 0, range: [0 -> 30])
  *
  * 每个分区打印30条记录
  * (null,{"rksj":"1563055723","latitude":"25.000000",
  * "imsi":"000000000000000","accept_message":"","phone_mac":"aa-aa-aa-aa-aa-aa",
  * "device_mac":"bb-bb-bb-bb-bb-bb","message_time":"1789098763",
  * "filename":"qq_source1_21111245.txt","phone":"18609765432",
  * "absolute_filename":"/usr/chl/data/filedir_successful/2019-07-14/data/filedir/qq_source1_21111245.txt",
  * "device_number":"32109231","imei":"000000000000000","collect_time":"1557305985",
  * "id":"f53096c5ff84491ea26d442a74db9303","send_message":"",
  * "object_username":"judy","table":"qq","longitude":"24.000000","username":"andiy"})
  */

/**
  * kafka-streaming 高阶API 自动维护offset 会有数据丢失
  */
object StreamingKafkaTest extends Serializable with Logging{
  val topic = "chl_test8"
  def main(args: Array[String]): Unit = {

    val ssc = SparkContextFactory.SparkStreamingLocal("StreamingKafkaTest",
      30L,
      1)

    val kafkaParams = getKafkaParam(topic,"console-consumer-4")
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))


    //val value = new KafkaManager(kafkaParams).createJsonToMapStringDirectSteramWithOffset(ssc,Set(topic))

    kafkaDS.foreachRDD(rdd=>{
      // direct模式，获取rdd当中的offset 保存在内存中，程序重新启动从0开始
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(x=>{
        println("获取RDD中的偏移信息"+x)
      })

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
