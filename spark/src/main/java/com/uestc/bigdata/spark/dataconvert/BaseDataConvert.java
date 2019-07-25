package com.uestc.bigdata.spark.dataconvert;


import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 基本的数据类型转换
 */
public class BaseDataConvert {

    public static HashMap<String ,Object> MapStringToLong(Map<String ,String> map,String key,HashMap<String ,Object> resMap){
        String value = map.get(key);
        if(StringUtils.isNoneBlank(value)){
            resMap.put(key, Long.valueOf(value));
        }else{
            resMap.put(key,0L);
        }
        return resMap;
    }

    public static HashMap<String,Object> MapStringToDouble(Map<String ,String> map,String key,HashMap<String ,Object> resMap){

        String value = map.get(key);
        if(StringUtils.isNoneBlank(value)){
            resMap.put(key, Double.valueOf(value));
        }else{
            resMap.put(key,0.000000);
        }
        return resMap;
    }

    public static HashMap<String,Object> MapStringToString(Map<String ,String> map, String key, HashMap<String ,Object> resMap){
        String value = map.get(key);
        if(StringUtils.isNoneBlank(value)){
            resMap.put(key, (value));
        }else{
            resMap.put(key,"");
        }
        return resMap;
    }
}
