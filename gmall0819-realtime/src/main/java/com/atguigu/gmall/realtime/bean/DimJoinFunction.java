package com.atguigu.gmall.realtime.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author coderhyh
 * @create 2022-04-25 12:31
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    String getKey(T obj);
}

