package com.uestc.bigdata.spark.streaming.kafka.kafka2ES

import java.util

import com.uestc.bigdata.es.admin.AdminUtils
import com.uestc.bigdata.es.client.EsClient
import com.uestc.bigdata.spark.common.dataconvert.DataConvert
import com.uestc.bigdata.spark.streaming.kafka.Spark_es_ConfigUtil
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.EsSpark

object KafkaToEsJob extends Serializable with Logging{





  def insertIntoESbyDate(table:String, data: DStream[util.Map[String, String]]): Unit ={
    // 通过表名+事件 构建索引 和 es表
    val index_prefix=table
    // toDo 拿到esclient客户端
    val client = EsClient.getEsClient()
    // 解析 data
    data.foreachRDD(rdd=>{
      // 获得rdd区间的所有日期
        val days = getDays(rdd)
      // 对每个日期，创建索引。存数据
      days.par.foreach(day=>{
        var index=index_prefix+"_"+day
        // todo判断索引是否存在
        val bool=AdminUtils.indexExists(client,index)
        if(!bool){
          //ToDo创建索引
          val indexMapping=s"es/mapping/${index_prefix}.json"
          AdminUtils.builderIndexAndType(index,index,indexMapping,5,1)
          println("创建索引成功"+index)
        }
        val value = rdd.filter(x => {
          x.get("index_date").equals(day)
        }).map(r => {
          DataConvert.strMaptoESObjectMap(r)
        })

        EsSpark.saveToEs(value,index+"/"+index,Spark_es_ConfigUtil.getEsParam("id"))
        println(s"向${index}写入"+value.count()+"条数据成功")
      })

    })

  }


  def getDays(rdd: RDD[util.Map[String, String]]): Array[String] = {
    val days: Array[String] = rdd.map(x => {
      x.get("index_date")
    }).distinct().collect()
    days
  }

  /**
    * kafkaDS数据类型转换中后写入es
    * @param table
    * @param data
    */
  def insertToEs(table:String,data: DStream[util.Map[String, String]]): Unit ={
      val index=table

    data.foreachRDD(rdd=>{
      val esRDD = rdd.map(x => {
        DataConvert.strMaptoESObjectMap(x)
      })
      EsSpark.saveToEs(esRDD,index+"/"+index,Spark_es_ConfigUtil.getEsParam("id"))
      println(s"向${index}写入"+esRDD.count()+"条数据成功")
    })
  }
}
