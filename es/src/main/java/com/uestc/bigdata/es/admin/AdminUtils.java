package com.uestc.bigdata.es.admin;

import com.uestc.bigdata.common.fileUtils.FileCommon;
import com.uestc.bigdata.es.client.EsClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * es简单工具类
 * 1.判断索引是否存在
 *
 * 2.创建索引
 *
 * 3.创建索引和表
 */
public class AdminUtils {
    private static Logger LOG = LoggerFactory.getLogger(AdminUtils.class);

    public static boolean indexExists(TransportClient esClient,String index){
        boolean isEsists=false;
        // 1 创建索引
        try {
            IndicesExistsResponse existsResponse = esClient.admin().indices().prepareExists(index).execute().actionGet();
            isEsists = existsResponse.isExists();
        } catch (Exception e) {
            LOG.error("判断index是否存在失败...");
        }
        return isEsists;
    }

    /**
     * 创建索引
     * @param esClient   客户端
     * @param index     索引名
     * @param shard          分片
     * @param replication     副本
     * @return
     */
    public static boolean createExists(TransportClient esClient,String index,int shard , int replication){
       if(!indexExists(esClient,index)){
           LOG.info("该index不存在，创建...");
           try {
               CreateIndexResponse createIndexResponse=null;
               createIndexResponse = esClient.admin().indices().prepareCreate(index)
                       .setSettings(Settings.builder()
                               .put("index.number_of_shards", shard)
                               .put("index.number_of_replicas", replication)
                               .put("index.codec", "best_compression")
                               .put("refresh_interval", "30s"))
                       .execute().actionGet();
               return createIndexResponse.isAcknowledged();
           } catch (Exception e) {
               LOG.error(null, e);
               return false;
           }
       }
        LOG.warn("该index " + index + " 已经存在...");
        return false;
    }


    /**
     * 动态创建索引
     * @param index
     * @param path
     * @param shard
     * @param replication
     * @return
     */
        public static boolean builderIndexAndType(String index,String type,String path,int shard , int replication) throws IOException {
            Boolean flag;
            TransportClient esClient=EsClient.getEsClient();
            String mappingJson = FileCommon.getAbstractPath(path);

            boolean exists = AdminUtils.createExists(esClient, index, shard, replication);
            if(exists){
                LOG.info("创建索引"+ index + "成功");
                flag = MappingUtil.addMapping(esClient, index, type, mappingJson);
            }else{
                LOG.error("创建索引"+ index + "失败");
                flag = false;
            }
            return flag;
        }


    public static void main(String[] args) throws IOException {
        TransportClient esClient = EsClient.getEsClient();
       /* boolean wechat = indexExists(esClient, "wechat");
        System.out.println(wechat);*/

       boolean qqqq = createExists(esClient, "qqqq", 5, 2);
        System.out.println(qqqq);

        boolean b = builderIndexAndType("ccc", "ccc", "es/mapping/qq.json"
                , 4, 2);
        System.out.println(b);
    }
}
