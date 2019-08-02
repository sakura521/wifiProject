package com.uestc.bigdata.spark.streaming.kafka2Hbase


import java.util

import com.uestc.bigdata.hbase.insert.HBaseInsertHelper
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

object KafkaInsertHbase extends Serializable with Logging{


  def insertHbase(wifiDS:DStream[java.util.Map[String,String]], table_name:String): Unit ={
    wifiDS.foreachRDD(rdd=>{
      val putRDD = rdd.map(x => {
        //测试
        println("kafka读取数据：=============="+x)

        val rowkey = x.get("id")
        var put = new Put(rowkey.getBytes())
        val keys = x.keySet()
        keys.foreach(key=>{
          put.addColumn("cf".getBytes(),Bytes.toBytes(key),Bytes.toBytes(x.get(key)))
        })
        put
      })
      putRDD.foreachPartition(partition=>{
       val list=new util.ArrayList[Put]()
        while(partition.hasNext){
          val put=partition.next()
          list.add(put)
        }
        HBaseInsertHelper.put(table_name,list,1000)
        logInfo("批量写入"+table_name+":"+list.size()+"条数据成功")
      })
    })
  }
}
