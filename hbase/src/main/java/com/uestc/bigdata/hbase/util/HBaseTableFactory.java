package com.uestc.bigdata.hbase.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * 管理表
 *  HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
 * 拿到表对象
 *  Table t = connection.getTable(TableName.valueOf(tableName));
 */
public class HBaseTableFactory {

    private static final long serialVersionUID = -1071596337076137201L;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTableFactory.class);

    private static HBaseConf hBaseConf;
    private transient Connection conn;
    private boolean isReady=true;

    public HBaseTableFactory(){
        hBaseConf=HBaseConf.getInstance();
        if(true){
            conn=hBaseConf.getHConnection();
        }else{
            isReady = false;
            LOG.warn("HBase 连接没有启动。");
        }
    }

    public HBaseTableFactory(Connection conn){this.conn=conn;}


    /**
     * 根据表名创建表的实例
     * @param tableName
     * @return
     * @throws IOException
     */
    public Table getHBaseInstance(String tableName) throws IOException {
        if(conn==null){
            if(hBaseConf==null){
                hBaseConf= HBaseConf.getInstance();
                isReady=true;
                LOG.warn("HBaseConf为空，重新初始化。");
            }
            synchronized (HBaseTableFactory.class){
                if(conn==null){
                    conn=hBaseConf.getHConnection();
                    LOG.warn("初始 hbase Connection 为空 ， 获取  Connection成功。");
                }
            }
        }
        return isReady?conn.getTable(TableName.valueOf(tableName)):null;
    }


    public void close() throws IOException {
        conn.close();
        conn=null;
    }


}
