package com.uestc.bigdata.spark.streaming.kafka2Hbase

import java.util.Properties

/**
  * xxxx******kafka最早的offset================Map([chl_test8,2] -> LeaderOffset(linux4,9092,0), [chl_test8,1] -> LeaderOffset(linux6,9092,0), [chl_test8,0] -> LeaderOffset(linux5,9092,0))
  * 打印分区信息
  * [chl_test8,2]
  * [chl_test8,1]
  * [chl_test8,0]
  * 打印消费者分区偏移信息
  * ([chl_test8,2],1306)
  * ([chl_test8,1],1409)
  * ([chl_test8,0],1389)
  * 获取spark 中的偏移信息OffsetRange(topic: 'chl_test8', partition: 2, range: [1306 -> 1346])
  * 获取spark 中的偏移信息OffsetRange(topic: 'chl_test8', partition: 1, range: [1409 -> 1449])
  * 获取spark 中的偏移信息OffsetRange(topic: 'chl_test8', partition: 0, range: [1389 -> 1429])
  * 批量写入test:chl_test8:40条数据成功
  *
  */


import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.hbase.split.SplitskeyRegion
import com.uestc.bigdata.hbase.util.HBaseTableUtil
import com.uestc.bigdata.spark.common.SparkContextFactory
import com.uestc.bigdata.spark.streaming.kafka.KafkaParamerUtil
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * kafka 数据全量导入HBase
  */
object KafkaToHbaseAll extends Serializable with Logging{

  private val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics="chl_test8"

  def main(args: Array[String]): Unit = {

    val hbase_table="test:chl_test8"
    //1.创建hbase表  create "tablename'  "column family"
    HBaseTableUtil.createTable(hbase_table,"cf",true,172800,1,SplitskeyRegion.getSplitsRowkerDist)
 //   HBaseTableUtil.truncateTable(hbase_table)
    //2.创建ssc
    val ssc = SparkContextFactory.SparkStreamingLocal("KafkaToHbaseAll",40L,1)

    //3.创建DStream
    val kafkaDS = new KafkaManager(KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"), "xiaofei"))
      .createJsonToMapStringDirectSteramWithOffset(ssc, Set(topics))
    //4.数据写道hbase表中
    KafkaInsertHbase.insertHbase(kafkaDS,hbase_table)

    ssc.start()
    ssc.awaitTermination()
  }
}
