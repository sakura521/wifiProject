package com.uestc.bigdata.hbase.insert;

import java.util.Iterator;

/**
 * 创建多线程插入的异常
 */
public class HBaseInsertExceptor extends Exception {
    public HBaseInsertExceptor(String message){super(message);}

    public final synchronized void addSuppresseds(Iterable<Exception> exceptions){
            if(exceptions!=null){
                Iterator<Exception> iterator = exceptions.iterator();
                while(iterator.hasNext()){
                    addSuppressed(iterator.next());
                }
            }
    }

}
