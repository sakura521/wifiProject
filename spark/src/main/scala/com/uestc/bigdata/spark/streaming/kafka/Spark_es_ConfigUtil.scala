package com.uestc.bigdata.spark.streaming.kafka

import org.apache.spark.Logging

object Spark_es_ConfigUtil extends Serializable with Logging{

  val ES_NODES="es.nodes"
  val ES_PORT="es.port"
  val ES_CLISTERNAME="es.clustername"
  def getEsParam(id_field : String):  Map[String,String] ={
    Map[String,String](
      "es.mapping.id"->id_field,
      ES_NODES->"linux4,linux5,linux6",
      ES_PORT->"9200",
      ES_CLISTERNAME->"es-Cluster",
      "es.batch.size.entries"->"6000",
      "es.nodes.discovery"->"true",
      "es.batch.size.bytes"->"300000000",
      "es.batch.write.refresh"->"false"
    )
  }
}
