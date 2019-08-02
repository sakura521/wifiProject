package com.uestc.bigdata.hbase.util;


import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * 创建HBaseconf
 * Configuration conf = HBaseConfiguration.create();
 * Connection connection = ConnectionFactory.createConnection(conf);
 */
public class HBaseConf implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HBaseConf.class);

    private static final String HBASE_SERVER_CONFIG="hbase/hbase-server-config.properties";
    private static final String HBASE_SITE="hbase/hbase-site.xml";

    //私有的对象 static 类加载时其存在  双重校验加volatile
    private volatile static HBaseConf hbaseConf;

    private CompositeConfiguration hbase_server_config;

    public CompositeConfiguration getHbase_server_config() { return hbase_server_config; }

    public void setHbase_server_config(CompositeConfiguration hbase_server_config) {
        this.hbase_server_config = hbase_server_config;
    }

    private Configuration configuration;
    private volatile transient Connection conn;

    /**
     * 私有类 其他处的代码就无法通过调用该类的构造函数来实例化该类的对象，
     * 只有通过该类提供的静态方法来得到该类的唯一实例。
     */
    private HBaseConf(){
        hbase_server_config=new CompositeConfiguration();
        lodConfig(HBASE_SERVER_CONFIG,hbase_server_config);
        getHConnection();
    }

    /**
     * 获得hbaseconn
     */
    public Connection getHConnection() {
       if(conn==null){
           getConfiguration();
           synchronized (HBaseConf.class){
               if(conn==null){
                   try {
                       conn = ConnectionFactory.createConnection(configuration);
                   } catch (IOException e) {
                       LOG.error(String.format("获取hbase的连接失败  参数为： %s", toString()), e);

                   }
               }
           }
       }
       return conn;
    }

    private Configuration getConfiguration() {
        if(configuration==null){
           configuration = HBaseConfiguration.create();
           configuration.addResource(HBASE_SITE);
            LOG.info("加载配置文件" + HBASE_SITE + "成功");
        }
        return configuration;
    }

    private void lodConfig(String path, CompositeConfiguration configuration) {

        try {
            LOG.info("加载配置文件 " + path);
            configuration.addConfiguration(new PropertiesConfiguration(path));
            LOG.info("加载配置文件" + path +"成功。 ");
        } catch (ConfigurationException e) {
            LOG.error("加载配置文件 " + path + "失败", e);
        }
    }

    /**
     * 获取唯一的conn方法:方法是静态的  由类名直接调用。内存中只存在一份
     * @return
     */
    public  static HBaseConf getInstance(){
        if(hbaseConf==null){
            synchronized (HBaseConf.class){
                if(hbaseConf==null){
                    hbaseConf=new HBaseConf();
                }
            }
        }
        return hbaseConf;
    }

    public static void main(String[] args) {
        Connection connection = HBaseConf.getInstance().getHConnection();
        System.out.println("conn========="+connection.toString());
    }
}
