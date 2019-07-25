package com.uestc.bigdata.hbase.bean;

/**
 * ROW                  COLUMN+CELL
 *  001                  column=info:age, timestamp=1544346025492, value=18
 */
public class HBaseRow extends AbstractRow<HBaseCell> {
    private String rowkey;

    /**
     * 借用抽象类的构造函数可以访问 抽象类的通用字段
     * @param rowkey
     */
    public HBaseRow(String rowkey){
        super(rowkey);
    }

    @Override
    protected HBaseCell createCell(String field, String value, long capTime) {
        return new HBaseCell(field,value,capTime);
    }
}
