package com.uestc.bigdata.hbase.extractor;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * 解析row解析器
 */
public interface RowExtractor<T> {

    /**
     * 解析rowkey数据
     * @param result  get得到的结果
     * @param rowNum   解析的数量
     * @return
     * @throws IOException
     */
    T extractRowData(Result result,int rowNum) throws IOException;
}
