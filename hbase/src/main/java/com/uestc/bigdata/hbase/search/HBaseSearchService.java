package com.uestc.bigdata.hbase.search;

import com.uestc.bigdata.hbase.extractor.RowExtractor;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.List;

public interface HBaseSearchService {

    /**
     * 根据用户给定的get 查询单条语句 并按自定义的方式解析结果集
     * @param tablename
     * @param get
     * @param extractor
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T search(String tablename, Get get, RowExtractor<T> extractor) throws IOException;


    /**
     * 根据用户给定的List<Get> gets 查询多条语句 并按自定义的方式解析结果集
     * @param tablename
     * @param gets
     * @param extractor
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> List<T> search(String tablename,List<Get> gets,RowExtractor<T> extractor) throws  IOException;
}
