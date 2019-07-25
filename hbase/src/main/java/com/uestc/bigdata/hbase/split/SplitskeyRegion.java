package com.uestc.bigdata.hbase.split;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * 划分region的区域   刚开始region分区编号没有排序
 * 利用treeset进行region分区编号排序
 */
public class SplitskeyRegion {
    public static byte[][] getSplitsRowkerDist(){
        String[] regions={"1","a","b","c","d","4","5","6","e","f","2","3","7","8","9"};

        byte[][] splitsKey=new byte[regions.length][];

        TreeSet<byte[]> rows=new TreeSet<>(Bytes.BYTES_COMPARATOR);

        for(int i=0;i<regions.length;i++){
            rows.add(Bytes.toBytes(regions[i]));
        }

        // 迭代器
        Iterator<byte[]> iteratorRow = rows.iterator();
        int i=0;
        while(iteratorRow.hasNext()){
            byte[] tempRow = iteratorRow.next();
            iteratorRow.remove();
            splitsKey[i]=tempRow;
            i++;
        }
        return splitsKey;
    }

    public static void main(String[] args) {
        byte[][] bytes = getSplitsRowkerDist();
        for(byte[] b:bytes){
            for(byte r:b){
                System.out.println(r);
            }
            System.out.println(b.toString()+"...");
        }
    }
}
