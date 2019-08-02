package com.uestc.bigdata.flume.inceperctor;

import com.alibaba.fastjson.JSON;
import com.uestc.bigdata.common.dataType.DataTypeUtils;
import com.uestc.bigdata.flume.fields.MapFields;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class DataInceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(DataInceptor.class);
    // 获取字段值
    HashMap<String, ArrayList<String>> dataType=DataTypeUtils.dataType;
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        SimpleEvent eventNew = new SimpleEvent();
        try {
            LOG.info("拦截器Event开始执行");
            HashMap<String,String> map=parseEvent(event);
            if(map==null){
                return null;
            }
            String jsonString = JSON.toJSONString(map);
            LOG.info("拦截器推送数据到channel:" +jsonString);
            eventNew.setBody(jsonString.getBytes());
        } catch (Exception e) {
            LOG.error(null,e);
        }
        return eventNew;
    }


    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event>  list=new ArrayList<>();
        for(Event event:events){
            Event intercept = intercept(event);
            if(intercept!=null){
                list.add(intercept);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }


    /**
     * 解析数据 对应到字段
     * @param event
     * @return
     */
    private HashMap<String, String> parseEvent(Event event) {
        if(event==null){
            return null;
        }
        HashMap<String, String> map=new HashMap<>();
        String lines =new String( event.getBody(), Charsets.UTF_8);
        String filename = event.getHeaders().get(MapFields.FILENAME);
        String absolute_filename = event.getHeaders().get(MapFields.ABSOLUTE_FILENAME);

        String table = filename.split("_")[0];
        String[] split = lines.split("\t");
        ArrayList<String> fields = dataType.get(table);
        for(int i=0;i<fields.size();i++){
            map.put(fields.get(i),split[i]);
        }
        map.put(MapFields.ID, UUID.randomUUID().toString().replace("-",""));
        map.put(MapFields.ABSOLUTE_FILENAME,absolute_filename);
        map.put(MapFields.TABLE,table);
        map.put(MapFields.FILENAME,filename);
        return map;
    }

    // 构建builder实例
    public static class Builder implements Interceptor.Builder{
        @Override
        public void configure(Context context) {

        }

        @Override
        public Interceptor build() {
           return new DataInceptor();
        }
    }

}
