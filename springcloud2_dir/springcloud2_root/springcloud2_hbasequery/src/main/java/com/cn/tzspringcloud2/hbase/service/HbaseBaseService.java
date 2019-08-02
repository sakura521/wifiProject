package com.cn.tzspringcloud2.hbase.service;

import com.uestc.bigdata.hbase.bean.HBaseCell;
import com.uestc.bigdata.hbase.bean.HBaseRow;
import com.uestc.bigdata.hbase.extractor.MultiVersionRowExtrator;
import com.uestc.bigdata.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import com.uestc.bigdata.hbase.search.HBaseSearchService;
import com.uestc.bigdata.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author: KING
 * @description:   1.根据二级索引表test:phone rowkey:value 查找到对应的value
 *                  2. 上面的value存储着要查找的phone信息的全部数据 rowkey 在关联数据中查找
 *                  HBaseSearchServiceImpl 封装的HBase查询的接口
 * @Date:Created in 2019-06-11 13:09
 */
@Service
public class HbaseBaseService {
    private static Logger LOG = LoggerFactory.getLogger(HbaseBaseService.class);
    @Resource
    private HbaseBaseService hbaseBaseService;
    /**
     * 获取单列
     * @param table
     * @param rowkey
     * @return
     */
    public Set<String> getSingleColumn(String table,String rowkey){
        Set<String> search = null;
        try {
            HBaseSearchService baseSearchService = new HBaseSearchServiceImpl();
            Get get = new Get(rowkey.getBytes());
            Set set = new HashSet<String>();
            // 单列版本解析器 通过value偏移量提取 数据值保存在set中
            SingleColumnMultiVersionRowExtrator singleColumnMultiVersionRowExtrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set);
            search = baseSearchService.search(table, get, singleColumnMultiVersionRowExtrator);
        } catch (IOException e) {
            LOG.error(null,e);
        }
        System.out.println(search);
        return search;
    }

    /**
     *  获取单列多版本
     * @param table
     * @param rowkey
     * @param versions
     * @return
     */
    public Set<String> getSingleColumn(String table,String rowkey,int versions){
        Set<String> search = null;
        try {
            HBaseSearchService baseSearchService = new HBaseSearchServiceImpl();
            Get get = new Get(rowkey.getBytes());
            get.setMaxVersions(versions);
            Set set = new HashSet<String>();
            SingleColumnMultiVersionRowExtrator singleColumnMultiVersionRowExtrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set);
            search = baseSearchService.search(table, get, singleColumnMultiVersionRowExtrator);
        } catch (IOException e) {
            LOG.error(null,e);
        }
        System.out.println(search);
        return search;
    }

    /**
     * hbase 二级查找
     * 1. 从二级索引表中找到多版本rowkey
     * 2. 拿到二级索引表中得到的主关联表的rowkey，对这些rowkey进行遍历，获取主关联表中对应的多版本数据
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getRealtion(String field,String fieldValue){

        Map<String,List<String>> map = new HashMap<>();

        //首先查找索引表
        String table = "test:" + field;
        String indexRowkey = fieldValue;
//        HbaseBaseService hbaseBaseService = new HbaseBaseService();

        Set<String> relationRowkeys = hbaseBaseService.getSingleColumn(table, indexRowkey, 100);

        //封装List<Get> 遍历rowkey  批量get查询
        List<Get> list = new ArrayList<>();
        relationRowkeys.forEach(relationRowkey->{
            Get get = new Get(relationRowkey.getBytes());
            try {
                get.setMaxVersions(100);
            } catch (IOException e) {
                e.printStackTrace();
            }
            list.add(get);
        });

        MultiVersionRowExtrator multiVersionRowExtrator = new MultiVersionRowExtrator();
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();

        try {
            List<HBaseRow> search = hBaseSearchService.search("test:relation", list, multiVersionRowExtrator);
            search.forEach(hbaseRow->{
                Map<String, Collection<HBaseCell>> cellMap = hbaseRow.getCell();
                cellMap.forEach((key,value)->{
                    List<String> listValue = new ArrayList<>();
                    value.forEach(x->{
                        listValue.add(x.toString());
                    });
                    map.put(key,listValue);
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(map.toString());
     return map;
    }


    public static void main(String[] args) {
        HbaseBaseService HbaseBaseService = new HbaseBaseService();
        HbaseBaseService.getRealtion("phone","18609765432");
    }

}
