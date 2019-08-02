package com.uestc.bigdata.es.admin;


import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingUtil {

    private static Logger LOG = LoggerFactory.getLogger(MappingUtil.class);


    /**
     * 动态添加mapping设置
     * @param client
     * @param index
     * @param type
     * @param mappingJson
     * @return
     */
    public static boolean addMapping(TransportClient client, String index, String type, String  mappingJson){
        PutMappingResponse putMappingResponse=null;
        try {
            PutMappingRequest putMappingRequest = new PutMappingRequest(index).
                     type(type).source(JSON.parseObject(mappingJson));
            putMappingResponse=  client.admin().indices().putMapping(putMappingRequest).actionGet();

        } catch (Exception e) {
            LOG.error(null,e);
            e.printStackTrace();
            LOG.error("添加" + type + "的mapping失败....",e);
            return false;
        }

        Boolean success=putMappingResponse.isAcknowledged();
        if(success){
            LOG.info("创建" + type + "的mapping成功....");
            return success;
        }
        return success;
    }
}
