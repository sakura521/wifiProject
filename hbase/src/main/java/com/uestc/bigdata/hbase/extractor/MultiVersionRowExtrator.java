package com.uestc.bigdata.hbase.extractor;


import com.uestc.bigdata.hbase.bean.HBaseRow;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 多版本解析器:get tablename rowkey  返回HBaseRow
 */
public class MultiVersionRowExtrator implements RowExtractor<HBaseRow>{

    private HBaseRow hBaseRow;

    @Override
    public HBaseRow extractRowData(Result result, int rowNum) throws IOException {

        hBaseRow = new HBaseRow(Bytes.toString(result.getRow()));
        String field=null;
        String value=null;
        long capTime=0L;
        List<Cell> cells = result.listCells();
        for(Cell cell:cells){
            field=Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            capTime=cell.getTimestamp();
            hBaseRow.addCell(field,value,capTime);
        }
        return hBaseRow;
    }
}
