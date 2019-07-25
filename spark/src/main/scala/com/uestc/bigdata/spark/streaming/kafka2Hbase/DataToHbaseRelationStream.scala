package com.uestc.bigdata.spark.streaming.kafka2Hbase

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.hbase.insert.HBaseInsertHelper
import com.uestc.bigdata.hbase.split.SplitskeyRegion
import com.uestc.bigdata.hbase.util.HBaseTableUtil
import com.uestc.bigdata.spark.common.SparkContextFactory
import com.uestc.bigdata.spark.streaming.kafka.KafkaParamerUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * 关联字段二级索引表入库
  * 插入元素  put '表名'，'rowkey','列族：列名'，'值'  put tablename phone_mac cf:field value
  *  对表操作需要用HbaseAdmin
  *   Connection connection = ConnectionFactory.createConnection(conf);
  *  拿到表对象
  *   Table t = connection.getTable(TableName.valueOf(tableName));
  *  1.用put方式加入数据   :实例化put 用 rowkey
  *     Put p =  new Put(Bytes.toBytes(rowkey));
  * 2.加入数据         put.addColumn(cf,name,value)
  *     p.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
  *     t.put(p);         table.put(put)
  *"test:phone_mac"  840
  */
object DataToHbaseRelationStream {

  //1. 获得关联字段信息
  private val relationFlieds: Array[String] = ConfigUtil.getInstance().getProperties("spark/relation.properties")
    .getProperty("relationfield")
    .split(",")

  def main(args: Array[String]): Unit = {

    // 初始化hbase表
   // initHBaseTable(relationFlieds)

    // 2. 获得kafka配置  topic信息
    val kafkaConfig = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
    val topic="chl_test0"

   // val topic=args(0).split(",")

    val ssc = SparkContextFactory.SparkStreamingLocal("DataToHbaseRelationStream",10L,1)

    //3.创建流
    val kafkaDS = new KafkaManager(KafkaParamerUtil.getKafkaParam(kafkaConfig.getProperty("metadata.broker.list"),
      "consumer-0"),
      true)
      .createJsonToMapStringDirectSteramWithOffset(ssc, Set(topic))
      .persist(StorageLevel.MEMORY_AND_DISK)

    //4.消费流 进行存储

    kafkaDS.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        while(partition.hasNext){
          // 获得数据流
          val map = partition.next()
          // 获得主键 rowkey:mac
          var phone_mac:String = map.get("phone_mac")

          relationFlieds.foreach(fields=>{
            if(map.containsKey(fields)){
              //  创建主关联表
              val put = new Put(phone_mac.getBytes())
              val value=map.get(fields)
              val versionNum=(fields+value).hashCode() & Integer.MAX_VALUE
              put.addColumn("cf".getBytes(),Bytes.toBytes(fields),versionNum,Bytes.toBytes(value))
              HBaseInsertHelper.put("test:relation",put)
              println(s"往主关联表 test:relation 里面写入数据  rowkey=>${phone_mac} version=>${versionNum} 类型${fields} value=>${value}")

              //  创建二级索引表
              val tablename=s"test:${fields}"
              val put2 = new Put(value.getBytes())

              val versionNum_2=phone_mac.hashCode() & Integer.MAX_VALUE
              put2.addColumn("cf".getBytes(),Bytes.toBytes("phone_mac"),versionNum_2,Bytes.toBytes(phone_mac))
               HBaseInsertHelper.put(tablename,put2)
              println(s"往二级索表 ${tablename}里面写入数据  rowkey=>${value} version=>${versionNum_2} value=>${phone_mac}")
            }
          })
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 初始化HBase表 创建，删除
    * @param relationFlieds
    */
  def initHBaseTable(relationFlieds: Array[String]): Unit = {
    val relation_table="test:relation"
    HBaseTableUtil.createTable(relation_table,
                              "cf",
                              true,
                              -1,
                              100,
                              SplitskeyRegion.getSplitsRowkerDist)
    HBaseTableUtil.deleteTable(relation_table)

    relationFlieds.foreach(fields=>{
      val tablename=s"test:${fields}"
      HBaseTableUtil.createTable(tablename,
        "cf",
        true,
        -1,
        100,
        SplitskeyRegion.getSplitsRowkerDist)
      HBaseTableUtil.deleteTable(tablename)
    })
  }
}
