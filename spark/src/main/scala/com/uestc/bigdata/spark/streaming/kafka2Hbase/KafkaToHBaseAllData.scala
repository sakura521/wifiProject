package com.uestc.bigdata.spark.streaming.kafka2Hbase

import java.util
import java.util.Properties

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.hbase.insert.HBaseInsertHelper
import com.uestc.bigdata.hbase.split.SplitskeyRegion
import com.uestc.bigdata.hbase.util.HBaseTableUtil
import com.uestc.bigdata.spark.common.SparkContextFactory
import com.uestc.bigdata.spark.streaming.kafka.KafkaParamerUtil
import com.uestc.bigdata.spark.streaming.kafka2Hbase.DataToHbaseRelationStream.relationFlieds
import com.uestc.bigdata.spark.streaming.kafka2Hbase.KafkaInsertHbase.logInfo
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * kafka 数据全量导入HBase  mac作为rowkey
  */
object KafkaToHBaseAllData extends Serializable with Logging{
  private val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics="chl_test0"

  def main(args: Array[String]): Unit = {

    val hbase_table="test:chl_test0"
    //1.创建hbase表  create "tablename'  "column family"
    //测试1
   /* HBaseTableUtil.createTable(hbase_table,
      "cf",
      true,
      -1,
      100,
      SplitskeyRegion.getSplitsRowkerDist)*/
    //   HBaseTableUtil.truncateTable(hbase_table)
    //2.创建ssc
    val ssc = SparkContextFactory.SparkStreamingLocal("KafkaToHBaseAllData",40L,1)

    //3.创建DStream
    val kafkaDS = new KafkaManager(KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"), "consumer-0"))
      .createJsonToMapStringDirectSteramWithOffset(ssc, Set(topics))
        .persist(StorageLevel.MEMORY_AND_DISK)

    //4.消费流 进行存储
    kafkaDS.foreachRDD(rdd=>{
      val putRDD = rdd.map(x => {
        //测试
        println("kafka读取数据：=============="+x)

        val rowkey = x.get("phone_mac")
        var put = new Put(rowkey.getBytes())
        val keys = x.keySet()
        keys.foreach(key=>{
          val value=x.get(key)
          val versionNum=(key+value).hashCode() & Integer.MAX_VALUE
          put.addColumn("cf".getBytes(),Bytes.toBytes(key),versionNum,Bytes.toBytes(value))
          HBaseInsertHelper.put("test:relation",put)
          println(s"往全表 test:chl_test0 里面写入数据  rowkey=>${rowkey} version=>${versionNum} 类型${key} value=>${value}")
        })
        put
      })
      putRDD.foreachPartition(partition=>{
        val list=new util.ArrayList[Put]()
        while(partition.hasNext){
          val put=partition.next()
          list.add(put)
        }
        HBaseInsertHelper.put(hbase_table,list,1000)
        logInfo("批量写入"+hbase_table+":"+list.size()+"条数据成功")
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
