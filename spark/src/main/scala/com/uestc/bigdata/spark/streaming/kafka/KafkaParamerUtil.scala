package com.uestc.bigdata.spark.streaming.kafka

import org.apache.spark.Logging

object KafkaParamerUtil extends Serializable with Logging{

  def getKafkaParam(brokers:String,groupId:String): Map[String,String] ={
    val kafkaParamer=Map[String,String](
      "metadata.broker.list"->brokers,
      "auto.offset.reset"->"smallest",
      "group.id"->groupId,
      "refresh.leader.backoff.ms"->"100",
      "num.consumer.fetchers"->"8"
    )
    kafkaParamer
  }
}
