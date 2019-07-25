package com.uestc.bigdata.hbase.search;


import com.uestc.bigdata.hbase.extractor.RowExtractor;
import com.uestc.bigdata.hbase.util.HBaseTableUtil;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 实现查询的实体类
 * 查看指定rowkey值
 *      get '表名'，'rowkey'
 * 查看具体列值
 *      get '表名','rowkey','列族：列
 *
 *
 *
 *
 *      扫描指定的数据
 *     public static void getRow(String tableName,String rowkey) throws IOException {
 *         //对表操作需要用HbaseAdmin
 *         Connection connection = ConnectionFactory.createConnection(conf);
 *         //拿到表对象
 *         Table t = connection.getTable(TableName.valueOf(tableName));
 *
 *         //1.扫描指定数据需要实例Get
 *         Get g = new Get(Bytes.toBytes(rowkey));
 *         //2.可加过滤条件
 *         g.addFamily(Bytes.toBytes("info"));
 *
 *         Result rs = t.get(g);
 *         Cell[] cells = rs.rawCells();
 *         //3.遍历
 *         //遍历具体数据 解析
 *             for(Cell c:cells){
 *                 System.out.println("行键为：" + Bytes.toString(CellUtil.cloneRow(c)));
 *                 System.out.println("列族为：" + Bytes.toString(CellUtil.cloneFamily(c)));
 *                 System.out.println("值为：" + Bytes.toString(CellUtil.cloneValue(c)));
 *             }
 *
 *     }
 */
public class HBaseSearchServiceImpl implements HBaseSearchService, Serializable {

    private static final long serialVersionUID = -8657479861137115645L;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSearchServiceImpl.class);

    /**
     * 单条语句查询:内部使用的批量查询实现
     * @param tablename
     * @param get
     * @param extractor
     * @param <T>
     * @return
     * @throws IOException
     */

    @Override
    public <T> T search(String tablename, Get get, RowExtractor<T> extractor) throws IOException {
        T obj=null;
        List<T> lists = search(tablename, Arrays.asList(get), extractor);
        if(!lists.isEmpty()){
            obj=lists.get(0);
        }
        return obj;
    }


    /**
     * 多条语句查询
     * @param tablename
     * @param gets
     * @param extractor
     * @param <T>
     * @return
     * @throws IOException
     */
    @Override
    public <T> List<T> search(String tablename, List<Get> gets, RowExtractor<T> extractor) throws IOException {
       List<T> data=new ArrayList<>();
       search(tablename,gets,extractor,data);
        return data;
    }

    /**
     * 实现具体的查询
     * @param tablename
     * @param gets
     * @param extractor
     * @param data
     * @param <T>
     */
    private <T> void search(String tablename, List<Get> gets, RowExtractor<T> extractor, List<T> data) throws IOException {
        Table table = HBaseTableUtil.getTable(tablename);
        if(table!=null){
                Result[] results = table.get(gets);
                int n=0;
                T rowData =null;
                for(Result result:results){
                    if(!result.isEmpty()){
                        rowData=  extractor.extractRowData(result, n);
                        if(rowData!=null){
                            data.add(rowData);
                        }
                        n++;
                    }
                }
           HBaseTableUtil.close(table);
        }else{
            throw new IOException(" table  " + tablename + " is not exists ..");
        }
    }
}
