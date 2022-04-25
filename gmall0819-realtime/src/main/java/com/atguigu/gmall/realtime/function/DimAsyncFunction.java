package com.atguigu.gmall.realtime.function;

/**
 * @author coderhyh
 * @create 2022-04-25 12:29
 * 异步维度关联的类
 * 模板方法设计模式：在父类中定义实现某一个功能的核心算法的骨架，具体的实现延迟到子类中去做，
 * 在不改变父类核心算法骨架的前提下，每一个子类都可以有自己不同的实现。
 */

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private String tableName;

    private ExecutorService executorService;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //开启多个线程，发送异步请求，完成维度关联
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    //获取要关联的维度的key
                    String key = getKey(obj);
                    //根据维度的key获取维度对象
                    JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);
                    //将维度的属性补充到流中的对象上
                    if (dimJsonObj != null && dimJsonObj.size() > 0) {
                        join(obj, dimJsonObj);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("异步维度关联耗时:" + (end - start) + "毫秒");
                    resultFuture.complete(Collections.singleton(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("异步维度关联发生了异常~~~");
                }
            }
        });
    }
}
