package com.uestc.bigdata.hbase.service;


import com.uestc.bigdata.hbase.bean.HBaseCell;
import com.uestc.bigdata.hbase.bean.HBaseRow;
import com.uestc.bigdata.hbase.extractor.MultiVersionRowExtrator;
import com.uestc.bigdata.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import com.uestc.bigdata.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 微服务层实现查询接口服务
 * 1.getSignalColumn  :获得单列数据  get(rowkey,field)
 * 2.getSignalColumn  :获得单列多版本数据  get(rowkey,field)
 * 3.getMultiColumn  :获得rowkey所有数据  get(rowkey)：关联查询
 */
public class HBaseService {

    private static Logger LOG = LoggerFactory.getLogger(HBaseService.class);

    private HBaseService hBaseService;

    /**
     * 实现单列单版本查询
     * @param tablename
     * @param rowkey
     * @return
     */
    public Set<String> getSignalColumn(String tablename, String rowkey){
        Set<String> res=null;

        try {
            HBaseSearchServiceImpl searchService = new HBaseSearchServiceImpl();
            Get get = new Get(rowkey.getBytes());
            Set<String> set=new HashSet<>();
            SingleColumnMultiVersionRowExtrator extrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set);
            res = searchService.search(tablename, get, extrator);
        } catch (IOException e) {
            LOG.error(null,e);
        }
        System.out.println(res);
        return res;
    }


    public Set<String> getSignalColumn(String tablename,String rowkey,int versionNum){
        Set<String> res=null;
        try {
            Get get = new Get(rowkey.getBytes());
            get.setMaxVersions(versionNum);
            HBaseSearchServiceImpl hBaseSearchService = new HBaseSearchServiceImpl();
            Set<String> set=new HashSet<>();
            SingleColumnMultiVersionRowExtrator extrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set);
            res=hBaseSearchService.search(tablename,get,extrator);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }


    /**
     * 通过单个字段获得这个字段对应的全量信息
     * 1.  通过 field 获得 所有的fieldValue
     * 2.   封装成get
     * 3.   对每条语句进行解析转换
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getRealtion(String field,String fieldValue){
        Map<String,List<String>> map=new HashMap<>();

        // 首先查找索引表
        String tablename="test:"+field;
        String fieldvalue=fieldValue;
        Set<String> phone_macs = getSignalColumn(tablename, fieldvalue,100);

        System.out.println("1.=================================================");

        // 每条mac进行封装查找所有的数据
        HBaseSearchServiceImpl baseSearchService = new HBaseSearchServiceImpl();
        List<Get> lists=new ArrayList<>();

        phone_macs.forEach(mac->{
            Get get = new Get(mac.getBytes());
            try {
                get.setMaxVersions(100);
            } catch (IOException e) {
                e.printStackTrace();
            }
            lists.add(get);
        });

        System.out.println("2.=========================================");
        MultiVersionRowExtrator extrator = new MultiVersionRowExtrator();
        try {
            List<HBaseRow> search = baseSearchService.search("test:relation", lists, extrator);

            System.out.println("3.===================================");
            search.forEach(hBaseRow -> {
                Map<String, Collection<HBaseCell>> cellMap = hBaseRow.getCell();
                cellMap.forEach((key,value)->{
                    List<String> valueMap=new ArrayList<>();
                    value.forEach(x->{
                        valueMap.add(x.toString());
                    });
                    map.put(key,valueMap);
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(map.toString());
        return map;
    }



    /**
     * 通过单个字段获得这个字段对应的轨迹信息
     * 1.  通过 field 获得 所有的fieldValue
     * 2.   封装成get
     * 3.   对每条语句进行解析转换
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getLocus(String field,String fieldValue){
        Map<String,List<String>> map=new HashMap<>();

        // 首先查找索引表
        String tablename="test:"+field;
        String fieldvalue=fieldValue;
        Set<String> phone_macs = getSignalColumn(tablename, fieldvalue,100);
        System.out.println("1.索引表查找完毕======================================");

        //2. 每条mac进行封装查找需要的数据
        HBaseSearchServiceImpl baseSearchService = new HBaseSearchServiceImpl();
        List<Get> lists=new ArrayList<>();

        phone_macs.forEach(mac->{
            Get get = new Get(mac.getBytes());
            try {
                get.setMaxVersions(100);
            } catch (IOException e) {
                e.printStackTrace();
            }
            lists.add(get);
        });

        System.out.println("2.=========================================");
        MultiVersionRowExtrator extrator = new MultiVersionRowExtrator();
        try {
            List<HBaseRow> search = baseSearchService.search("test:chl_test0", lists, extrator);

            System.out.println("3.===================================");
            search.forEach(hBaseRow -> {
                Map<String, Collection<HBaseCell>> cellMap = hBaseRow.getCell();
                cellMap.forEach((key,value)->{
                    List<String> valueMap=new ArrayList<>();
                    value.forEach(x->{
                        valueMap.add(x.toString());
                    });
                    map.put(key,valueMap);
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String, List<String>> locus = getLocus(map);
        System.out.println(locus.toString());
        return locus;
    }


    /**
     * 传入的是查找的mac的所有全量数据
     * 需要根据时间过滤出字段信息
     * 查找最近3天的轨迹信息:没法根据时间过滤
     * @param map
     * @return
     */
    public Map<String,List<String>> getLocus(Map<String,List<String>> map){
        Map<String,List<String>> res=new HashMap<>();
        String[] includes = new String[]{"latitude","longitude","collect_time"};
        Set<String> keys = map.keySet();
        keys.forEach(key->{
            if(key.equals(includes[0]) || key.equals(includes[1])){
                res.put(key,map.get(key));
            }
        });
        return res;
    }


    public static void main(String[] args) {
        HBaseService sevice=new HBaseService();
     /*  Set<String> phone = sevice.getSignalColumn("test:username", "KZ-dE-KQ-RR-Yl-aS");
       for(String p:phone){
           System.out.println(p);
       }*/
//        Map<String, List<String>> qndiy = sevice.getLocus("username", "SVbqmHc");
        Map<String, List<String>> realtion = sevice.getRealtion("username", "SVbqmHc");
        realtion.forEach((key,value)->{
            System.out.println("key="+key+"\t"+"value="+value);
        });
    }
}
