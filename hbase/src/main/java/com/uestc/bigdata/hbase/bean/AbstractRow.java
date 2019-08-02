package com.uestc.bigdata.hbase.bean;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * hbase  的一条 rowkey信息 可能会存在多版本
 * 1个rowkey对应多个field
 * 1个field对象多个value/captime
 * @param <T>
 */
public abstract class AbstractRow<T extends HBaseCell>  {

    protected String rowkey;
    protected HashMultimap<String,T> cells;

    protected Set<String> fields;
    protected long maxCapTime;

    public  AbstractRow(String rowkey){
        this.rowkey=rowkey;
        cells=HashMultimap.create();
        fields= Sets.newHashSet();
    }

    public boolean addCell(String field,String value,long capTime){
        return addCell(field,createCell(field,value,capTime));
    }


    /**
     * 批量数据插入
     * @param field
     * @param cells
     * @return
     */
    public boolean[] addCells(String field, Collection<T> cells){
        boolean[] status=new boolean[cells.size()];
        int n=0;
        for(T cell:cells){
            status[n]=addCell(field,cell);
            n++;
        }
        return status;
    }


    /**
     * 单条数据插入
     * @param field
     * @param cell
     * @return
     */
    public boolean addCell(String field,T cell){
        fields.add(cell.getField());

        if(cell.getCapTime()>maxCapTime){
            maxCapTime=cell.getCapTime();
        }

        return cells.put(field,cell);
    }

    /**
     * 抽象类 createCell  :new HBaseCell
     * @param field
     * @param value
     * @param capTime
     * @return
     */
    protected abstract T createCell(String field, String value, long capTime);

    public String getRowkey() {
        return rowkey;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o){ return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        AbstractRow<?> that = (AbstractRow<?>) o;
        return rowkey.equals(that.rowkey);

    }
    @Override
    public int hashCode() {
        return this.rowkey.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractRow [rowKey=" + rowkey + ", cells=" + cells + "]";
    }

    public Map<String, Collection<T>> getCell(){return cells.asMap();}


}
