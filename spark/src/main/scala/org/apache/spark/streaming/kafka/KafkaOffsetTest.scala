package org.apache.spark.streaming.kafka

import org.apache.spark.Logging

object KafkaOffsetTest extends Serializable with Logging{

  val topic="chl_test8"

  private val kafkaParams: Map[String, String] = getKafkaParam(topic,"console-consumer-31622")

  // 实例化kafkacluster对象
  @transient
  private var cluster=new KafkaCluster(kafkaParams)

  def kc():KafkaCluster={
    if(cluster==null){
      cluster=new KafkaCluster(kafkaParams)
    }
    cluster
  }

  /**
    * partitionInfo= Right(Set([chl_test8,2], [chl_test8,1], [chl_test8,0]))
    * 打印分区信息
    * [chl_test8,2]
    * [chl_test8,1]
    * [chl_test8,0]
    * 打印kafka最早的分区offset信息
    * ([chl_test8,2],LeaderOffset(linux4,9092,0))
    * ([chl_test8,1],LeaderOffset(linux6,9092,0))
    * ([chl_test8,0],LeaderOffset(linux5,9092,0))
    * 打印kafka最晚的分区offset信息
    * ([chl_test8,2],LeaderOffset(linux4,9092,1306))
    * ([chl_test8,1],LeaderOffset(linux6,9092,1409))
    * ([chl_test8,0],LeaderOffset(linux5,9092,1389))
    * 打印consumerE分区offset信息
    * ([chl_test8,2],1306)
    * ([chl_test8,1],1409)
    * ([chl_test8,0],1389)
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // 一个topic可能有好几个分区
    val partitionInfo = kc.getPartitions(Set(topic))
    require(partitionInfo.isRight,s"获取partion失败")
    println("partitionInfo= "+partitionInfo)
    val partitions = partitionInfo.right.get
    println("打印分区信息")
    partitions.foreach(println(_))

    //打印kafka每个分区 中最早的offset

    val earliestOffsetE = kc.getEarliestLeaderOffsets(partitions)
    require(earliestOffsetE.isRight,s"获取earliestLeader失败")
    val earliestOffsets = earliestOffsetE.right.get
    println("打印kafka最早的分区offset信息")
    earliestOffsets.foreach(println(_))


    //打印kafka每个分区 中最晚的offset

    val LeastOffsetE = kc.getLatestLeaderOffsets(partitions)
    require(LeastOffsetE.isRight,s"获取LeastOffsetE失败")
    val LeastOffsetEs = LeastOffsetE.right.get
    println("打印kafka最晚的分区offset信息")
    LeastOffsetEs.foreach(println(_))

    //获取消费者组中的offset
    val consumerE = kc.getConsumerOffsets("console-consumer-31622",partitions)
    require(consumerE.isRight,s"获取consumerE失败")
    val consumerOffsets = consumerE.right.get

    println("打印consumerE分区offset信息")
    consumerOffsets.foreach(println(_))
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
