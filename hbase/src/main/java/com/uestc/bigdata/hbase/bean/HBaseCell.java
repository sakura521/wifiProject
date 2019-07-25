package com.uestc.bigdata.hbase.bean;

/**
 * 创建hbase 一个单元格信息  field  fieldValue  captime
 * 重写各种方法
 *  column=info:age, timestamp=1544346025492, value=18
 */
public class HBaseCell implements Comparable<HBaseCell> {


    protected String field;

    protected String fieldvalue;
    protected Long capTime;

    public HBaseCell(String field, String fieldvalue, Long capTime) {
        this.field = field;
        this.fieldvalue = fieldvalue;
        this.capTime = capTime;
    }

    public String getField() {
        return field;
    }

    public String getFieldvalue() {
        return fieldvalue;
    }

    public Long getCapTime() {
        return capTime;
    }

    public void setCapTime(Long capTime) {
        this.capTime = capTime;
    }

    @Override
    public String toString() {
        return String.format("%s_[%s]_%s",field,capTime,fieldvalue);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        HBaseCell cell = (HBaseCell) o;
       if(field.equals(cell.field)  && fieldvalue.equals(cell.fieldvalue)){
           if(capTime>cell.capTime){
               cell.setCapTime(capTime);
           }
           return  true;
       }
       return false;
    }

    @Override
    public int hashCode() {
        return this.field.hashCode()+31*this.fieldvalue.hashCode();
    }

  //1.按照时间戳降序排列
    @Override
    public int compareTo(HBaseCell o) {
        return o.getCapTime().compareTo(this.capTime);
    }
}
