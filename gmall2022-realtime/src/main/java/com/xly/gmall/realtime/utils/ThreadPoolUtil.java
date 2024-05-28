package com.xly.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 */
public class ThreadPoolUtil {
    //使用volatile关键字，会对其他线程可见这个线程改变的代码数据
    private static volatile ThreadPoolExecutor poolExecutor;
    public static ThreadPoolExecutor getInstance(){
        //判断线程是不是已经创建了线程池，如果已经创建了，就不创建了
        if (poolExecutor == null){
            //同步代码块上锁，在多线程中只能有一个线程在里面执行操作，需要创建一个线程池执行对象
            synchronized (ThreadPoolUtil.class){
                //需要再次判断，否则也会容易造成多次创建连接池
                if (poolExecutor == null){
                    System.out.println("~~~创建线程池连接对象~~~");
                    poolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return poolExecutor;
    }
}
