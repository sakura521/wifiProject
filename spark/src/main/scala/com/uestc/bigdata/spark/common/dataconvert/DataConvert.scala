package com.uestc.bigdata.spark.common.dataconvert

import java.util

import com.uestc.bigdata.common.ConfigUtil
import com.uestc.bigdata.spark.dataconvert.BaseDataConvert
import org.apache.spark.Logging

import scala.collection.JavaConversions._
object DataConvert extends Serializable with Logging{

  val FILEDMAPING = "es/mapping/fieldmapping.properties"
  private val TableFieldsType: util.HashMap[String, util.HashMap[String, String]] = getEsMappingToString

  /**
    * Map[String,String] =》Map[String,Object]
    * 流进来的数据：
    * {"latitude":"24.000000",
    * "imsi":"000000000000000",
    * "accept_message":"",
    * "phone_mac":"aa-aa-aa-aa-aa-aa"
    * ....}
    * 进行转换
    * @param map
    */
  def strMaptoESObjectMap(map:java.util.Map[String,String]): java.util.Map[String,Object] = {
    // 获取当前表
    val index = map.get("table")
    // 获取表对应的字段+类型
    val indexType = TableFieldsType.get(index)
    //提取出字段信息
    val indexFields = indexType.keySet()

    var objectMap = new util.HashMap[String, Object]()
    //  获取数据流的字段
    val dataFields = map.keySet().iterator()

    try {
      while (dataFields.hasNext) {
        val key = dataFields.next()
        var keyType: String = "String"

        if (indexType.containsKey(key)) {
          keyType = indexType.get(key)
        }
        keyType match {
          case "long" => objectMap = BaseDataConvert.MapStringToLong(map, key, objectMap)
          case "string" => objectMap = BaseDataConvert.MapStringToString(map, key, objectMap)
          case "double" => objectMap = BaseDataConvert.MapStringToDouble(map, key, objectMap)
          case _ => objectMap = BaseDataConvert.MapStringToString(map, key, objectMap)
        }
      }
    } catch {
      case e: Exception => logInfo("转换异常", e)
    }
    println("转换后" + objectMap)
    objectMap
  }




  /**
    * 获取配置文件的内容 转换成Map[String,[String,String]]形式与数据流形式相匹配
    * @return
    */
  def getEsMappingToString(): util.HashMap[String,util.HashMap[String,String]] ={
    // 获取数据名称+类型
    val fieldsTypes = ConfigUtil.getInstance().getProperties(FILEDMAPING)
    val resMap = new util.HashMap[String, util.HashMap[String, String]]
    //2.获取表名
    val tables = fieldsTypes.getProperty("tables").split(",")
    val tableMappings = fieldsTypes.keySet()

    tables.foreach(table=>{
      val map=new util.HashMap[String,String]()
      tableMappings.foreach(tableMapping=>{
        if(tableMapping.toString.startsWith(table)){
          val key=tableMapping.toString.split("\\.")(1)
          val value=fieldsTypes.get(tableMapping).toString
          map.put(key,value)
        }
      })
      resMap.put(table,map)
    })
    resMap
  }
}
