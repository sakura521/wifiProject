package com.uestc.bigdata.hbase.insert;

import com.google.common.collect.Lists;
import com.uestc.bigdata.hbase.util.HBaseTableUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * hbase数据插入
 */
public class HBaseInsertHelper implements Serializable {

    /**
     * 单条数据插入
     * @param tablename
     * @param put
     */
    public  static void put(String tablename, Put put) throws  Exception{
        put(tablename, Lists.newArrayList(put));
    }

    /**
     * 数据批量插入
     * table.put(puts);
     */

    public static void put(String tablename, List<Put> puts){
        if(!puts.isEmpty()){
            Table table = HBaseTableUtil.getTable(tablename);
            try {
                table.put(puts);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                HBaseTableUtil.close(table);
            }
        }
    }

    /**
     * 多线程进行插入
     * @param tablename  ：表名
     * @param puts      ：要批量插入的数据  一个RDD的一个partition进行插入
     * @param preThreadNum      ：预先设置的线程任务个数（每个线程处理的任务数）
     */
    public static void put(final String tablename,List<Put> puts,int preThreadNum) throws Exception {

        int size=puts.size();

        if(size>preThreadNum){
            int threadNum=(int) Math.ceil(size/preThreadNum);
            ExecutorService services = Executors.newFixedThreadPool(threadNum);
           final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
            List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
            try {
                // 切分任务  i代表第几个线程
                for(int i=0;i<threadNum;i++){
                    final List<Put> temp;
                    // 数据切分
                    if(i==(threadNum-1)){
                        temp=puts.subList(preThreadNum*i,size);
                    }else{
                        temp=puts.subList(preThreadNum*i,preThreadNum*(i+1));
                    }
                    services.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if(exceptions.isEmpty()){
                                    put(tablename,temp);
                                }
                            } catch (Exception e) {
                                exceptions.add(e);
                            } finally {
                                countDownLatch.countDown();
                            }
                        }
                    });
                }
                countDownLatch.await();
            } finally {
                services.shutdown();
            }

            if(exceptions.size()>0){
                HBaseInsertExceptor insertExceptor = new HBaseInsertExceptor(String.format("put 数据到表%s失败", tablename));
                insertExceptor.addSuppresseds(exceptions);
                throw insertExceptor;
            }

        }else {
            put(tablename, puts);
        }
    }
}
