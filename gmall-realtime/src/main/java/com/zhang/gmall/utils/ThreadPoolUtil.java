package com.zhang.gmall.utils;

import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @title: 线程池
 * @author: zhang
 * @date: 2022/3/10 21:35
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPool;

      /*
      corePoolSize:初始线程数量
      maximumPoolSiz：最大连接数
      keepAliveTime：当空闲线程超过corePoolSize时。多余的线程会在多长时间销毁
     */
    public static ThreadPoolExecutor getThreadPool(){
        if (threadPool==null){
            synchronized (ThreadPoolUtil.class){
                if (threadPool==null){
                    threadPool = new ThreadPoolExecutor(
                            6,20,300L, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return threadPool;
    }
}
