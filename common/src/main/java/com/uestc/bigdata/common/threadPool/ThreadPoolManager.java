package com.uestc.bigdata.common.threadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 创建一个线程池管理器 单例模式
 */
public class ThreadPoolManager {
    private static final long serialVersionUID = 1465361469484903957L;

    private static ThreadPoolManager tpm;
    private transient ExecutorService newCacheThreadPool;
    private  int poolcapicity;
    private transient ExecutorService newFixThreadPool;
    private ThreadPoolManager(){}

    public static ThreadPoolManager getInstance(){
        if(tpm==null){
            synchronized (ThreadPoolManager.class){
                if(tpm==null){
                    tpm=new ThreadPoolManager();
                }
            }
        }
        return tpm;
    }

    public ExecutorService getExecutorServer(){
        if(newCacheThreadPool==null){
            synchronized (ThreadPoolManager.class){
                if(newCacheThreadPool==null){

                    newCacheThreadPool = Executors.newCachedThreadPool();
                }
            }
        }
        return newCacheThreadPool;
    }

    public ExecutorService getExecutorServer(int poolcapcity){
        return getExecutorService(poolcapcity,false);
    }

    private synchronized ExecutorService getExecutorService(int poolcapcity, boolean clodeOld) {
        if(newFixThreadPool==null || this.poolcapicity!=poolcapcity){
            if(newFixThreadPool!=null && clodeOld){
                newFixThreadPool.shutdown();
            }

            newFixThreadPool=Executors.newFixedThreadPool(poolcapcity);
            this.poolcapicity=poolcapcity;
        }
        return newFixThreadPool;
    }
}
