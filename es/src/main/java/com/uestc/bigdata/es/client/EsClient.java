package com.uestc.bigdata.es.client;

import com.uestc.bigdata.common.ConfigUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;

public class EsClient implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(EsClient.class);
    private volatile static TransportClient esClient;

    private EsClient(){

    }
    private static Properties properties;
    static {
        properties = ConfigUtil.getInstance().getProperties("es/es_cluster.properties");
    }

    public static TransportClient getEsClient(){
        System.setProperty("es.set.netty.runtime.available.processors","false");
        String esCluster = properties.getProperty("es.cluster.name");
        String esClusterNode1 = properties.getProperty("es.cluster.nodes1");
        String esClusterNode2 = properties.getProperty("es.cluster.nodes2");
        String esClusterNode3 = properties.getProperty("es.cluster.nodes3");
        LOG.info("clusterName:"+ esCluster);
        LOG.info("clusterNodes:"+ esClusterNode1);
        LOG.info("clusterNodes:"+ esClusterNode2);
        LOG.info("clusterNodes:"+ esClusterNode3);

        if(esClient==null){
            synchronized (EsClient.class){
                if(esClient==null){
                    try {
                        Settings settings = Settings.builder()
                                .put("cluster.name", esCluster)
                                .put("client.transport.sniff", true).build();

                        esClient = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(esClusterNode1), 9300))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(esClusterNode2), 9300))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(esClusterNode3),9300));
                        LOG.info("esClient========" + esClient.listedNodes());
                    } catch (Exception e) {
                        LOG.error("获取客户端失败",e);
                    }

                }
            }
        }
        return esClient;
    }

    /**
     * 关闭客户端
     */
    public static void close(){
        if(esClient!=null){
            esClient.close();
        }
    }

    public static void main(String[] args) {
        TransportClient client = EsClient.getEsClient();
        System.out.println(client);
    }
}
