package org.apache.spark.streaming.kafka

import java.util

import com.alibaba.fastjson.TypeReference
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.reflect.ClassTag

/**
  * 手动维护kafka offset到zookeeper中，直连方式
  */
class KafkaManager (val kafkaParams:Map[String,String],
                    val autoUpdateOffset: Boolean=true) extends Serializable  with Logging {

  @transient
  private var cluster = new KafkaCluster(kafkaParams)

  def kc(): KafkaCluster = {
    if (cluster == null) {
      cluster = new KafkaCluster(kafkaParams)
    }
    cluster
  }


  def createDirectStream[K: ClassTag, V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext, topics: Set[String]):InputDStream[(K, V)] = {

    //1.获取消费者组
    val consumer = kafkaParams.get("group.id").getOrElse("default")

    println("======================"+consumer)



    setOrUpdateOffset(topics,consumer)

    //2. 更新成功之后  获取数据消费
    val message={

        val partitionE = kc.getPartitions(topics)
        val partitions = partitionE.right.get
        println("打印分区信息")
        partitions.foreach(println(_))

        val consumerE = kc.getConsumerOffsets(consumer,partitions)
        val consumerOffset = consumerE.right.get
        println("打印消费者分区偏移信息")
        consumerOffset.foreach(println(_))

        //创建消费流
        KafkaUtils.createDirectStream[K,V,KD,VD,(K,V)](ssc,
          kafkaParams,
          consumerOffset,
          (mmd:MessageAndMetadata[K,V])=>(mmd.key(),mmd.message()))
    }

    if(autoUpdateOffset){
      // 消费完数据，更新offset
      message.foreachRDD(rdd=>{
        logInfo("RDD 消费成功，开始更新zookeeper上的偏移")
        upDateOffset(rdd)
      })
    }
    message
  }

  /**
    * 开始运行数据流的时候更新offset
    * @param topics
    * @param consumer
    */
  def setOrUpdateOffset(topics:Set[String],consumer:String): Unit ={

    topics.foreach(topic=>{

      //1.获取kafkapartiton信息
      val partitionsE = kc.getPartitions(Set(topic))
      logInfo("partitionsE= "+partitionsE)
      require(partitionsE.isRight,s"获取partition信息失败")
      val partitions = partitionsE.right.get

      //2. 获取最早的kafka offset
      val earlieastE = kc.getEarliestLeaderOffsets(partitions)
   /*   require(earlieastE.isRight,s"获取earlieastE信息失败")
      val earlieastOffset = earlieastE.right.get*/

      //3. 获取最晚的kafka offset
      val LeastOffsetE = kc.getLatestLeaderOffsets(partitions)
     /* require(LeastOffsetE.isRight,s"获取LeastOffsetE信息失败")
      val leastOffsetOffset = LeastOffsetE.right.get
*/

      //4.获取消费者组的offset

      val consumerE = kc.getConsumerOffsets(consumer,partitions)
      // 不去掉会报错
     /* require(consumerE.isRight,s"获取consumerE信息失败")
      val consumerOffset = consumerE.right.get*/

      //5.如果zookeeper中的consumerOffset存在
      if(consumerE.isRight) {
        //6.获取kafka最早的offset,如果存在
        if (earlieastE.isRight) {
          val leastOffset = earlieastE.right.get
          println("kafka最早的offset================"+leastOffset)
          //7.获取zookeeper中的offset
          val consumerOffset = consumerE.right.get
          //8.对每个分区的offset单独进行更新，因为有些过时，有些没有
          var offsets: Map[TopicAndPartition, Long] = Map()

          //9.对消费者中的每个分区进行更新
          consumerOffset.foreach({
            case (tp, n) => {
              val leastoffset = leastOffset(tp).offset
              //10 使用map集合保存将要更新的offset
              if (n < leastoffset) {
                logWarning("consumer group:" + consumerOffset + ",topic:" + tp.topic + ",partition:" + tp.partition +
                  " offsets已经过时，更新为" + leastoffset)
                offsets += (tp -> leastoffset)
            }
            }
          })
          setOffset(consumer, offsets)
        }
      }else{
            //2. zookeeper 中的offset 不存在
            //3.如果leastoffset还没有消费过，异常报错
            if(earlieastE.isLeft)
              logError(s"${topic} hasConsumed but earliestLeaderOffsets is null.")

           //4.存在过 判断是否从头消费
            val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase).getOrElse("smallest")
              //5.map构建消费者偏移
            var offset: Map[TopicAndPartition, Long] = Map.empty
            if (reset.equals("smallest")) {
                if (earlieastE.isRight) {
                  offset = earlieastE.right.get.map {
                    case (tp, n) => (tp, n.offset)
                  }

                } else {
                  offset = partitions.map(tp => (tp, 0L)).toMap
                }
              } else{
                offset = kc.getLatestLeaderOffsets(partitions).right.get.map{
                  case(tp,n)=>(tp,n.offset)
                }
              }
              println("更新的offset====================="+offset)
              setOffset(consumer,offset)
            }
      })
  }

  /**
    * 通过kafkaCluster更新offset信息
    * @param groupId
    * @param offsets
    */
  def setOffset(groupId: String, offsets: Map[TopicAndPartition, Long]): Unit ={
      if(offsets.nonEmpty){
        val updateOffset = kc.setConsumerOffsets(groupId,offsets)
        logInfo(s"更新zookeeper中的消费者信息为:${groupId} 的topic offset信息 ${updateOffset}")
        if(updateOffset.isLeft){
          logError(s"Error updating the offset to Kafka cluster:${updateOffset.right.get}")
        }
      }
  }


  /**
    *  通过spark的RDD 更新zookeeper上的消费offsets
    */

  def upDateOffset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Unit = {
      val groupId = kafkaParams.get("group.id").getOrElse("default")

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(x=>println("获取spark 中的偏移信息"+x))
      for(offset<- offsetRanges){
        //根据topic和partition 构建topicAndPartition
        val topicAndPartition = TopicAndPartition(offset.topic,offset.partition)
        logInfo("将SPARK中的 偏移信息 存到zookeeper中")
        setOffset(groupId,Map((topicAndPartition,offset.untilOffset)))
    }
  }




  /**
    * 创建 json 到 string 的map数据流 转换器 对value进行转换
    */

  def createJsonToMapStringDirectSteramWithOffset(ssc:StreamingContext,topics:Set[String]): DStream[java.util.Map[String,String]] = {
    val converter = { json: String =>
          var res:java.util.Map[String,String]=null
          try {
            res = com.alibaba.fastjson.JSON.parseObject(json, new TypeReference[util.Map[String, String]]() {})
          } catch {
            case  e:Exception=>logError(s"解析topic ${topics}, 的记录 ${json} 失败。", e)
          }
        res
      }
        createDirectStreamWithOffset(ssc,topics,converter).filter(_ != null)
}


  /**
    *根据converter创建流数据
    * @param ssc
    * @param topics
    * @param converter
    * @tparam T
    * @return
    */
  def createDirectStreamWithOffset[T:ClassTag](ssc: StreamingContext, topics: Set[String], converter: String => T): DStream[T]  = {
    createDirectStream[String, String, StringDecoder, StringDecoder](ssc, topics)
      .map(lines=>converter(lines._2))
  }

}