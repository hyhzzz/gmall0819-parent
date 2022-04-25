package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author coderhyh
 * @create 2022-04-25 12:33
 * 线程池工具类
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor poolExecutor;

    //获取线程池
    public static ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (poolExecutor == null) {
                    System.out.println("~~~创建线程池对象~~~");
                    poolExecutor =
                            new ThreadPoolExecutor(
                                    4,
                                    20,
                                    300,
                                    TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }

}
