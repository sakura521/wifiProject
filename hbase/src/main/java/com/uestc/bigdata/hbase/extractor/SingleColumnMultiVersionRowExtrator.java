package com.uestc.bigdata.hbase.extractor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * 单列多版本解析器:  get tablename rowkey cf:cn
 */
public class SingleColumnMultiVersionRowExtrator implements RowExtractor<Set<String>>{

    private Set<String> values;
    private byte[] cf;
    private byte[] cn;

    /**
     *
     * @param values   解析后返回的值
     * @param cf   列族
     * @param cn   列名
     */
    public  SingleColumnMultiVersionRowExtrator(byte[] cf,byte[] cn,Set<String> values){
        this.values=values;
        this.cf=cf;
        this.cn=cn;
    }


    // cell.getValueArray() 获取这条数据的全部信息
    // cell.getValueOffset() 对应上条信息的偏移量  value偏移量的起始位置
    // cell.getValueLength() 从上面的偏移量移动多少取字段  value偏移量的长度

    @Override
    public Set<String> extractRowData(Result result, int rowNum) throws IOException {

        List<Cell> cells = result.getColumnCells(cf, cn);
        for(Cell cell:cells){
            values.add(Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        }
        return values;
    }
}
