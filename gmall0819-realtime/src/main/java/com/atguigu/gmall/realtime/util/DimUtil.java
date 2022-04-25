package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-23 23:33
 * 查询维度工具类
 */
public class DimUtil {

    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("ID", id));
    }

    /**
     * 获取维度数据  --- 加了旁路缓存优化
     * 旁路缓存：先从Redis缓存中获取数据，如果获取到了，直接将获取到的数据作为维度数据进行返回；
     * 如果没有从Redis中获取到维度数据的话，那么发送请求，到Phoenix表中获取维度数据，并将获取到的维度数据缓存到Redis中
     * Redis
     * 类型： String
     * TTL:  1day
     * key:  dim:维度表名:主键值1_主键值2
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接查询Redis的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");

        //拼接查询语句 select * from dim_base_trademark where id ='12'
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");

        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='" + columnValue + "' ");
            redisKey.append(columnValue);
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
                redisKey.append("_");
            }
        }

        Jedis jedis = null;
        String dimStr = null;
        JSONObject dimJsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            dimStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("从Redis缓存中查询数据发生异常~~");
        }
        //判断是否从Redis中获取到了维度数据
        if (dimStr != null && dimStr.length() > 0) {
            //缓存命中
            dimJsonObj = JSON.parseObject(dimStr);
        } else {
            //没有从Redis中查到数据，发送请求到Phoenix中查询
            System.out.println("从Phoenix表中查询数据的SQL:" + selectSql);
            List<JSONObject> jsonObjList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);

            if (jsonObjList != null && jsonObjList.size() > 0) {
                dimJsonObj = jsonObjList.get(0);
                //将查询出来的维度数据，放到Redis中缓存起来
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("在维度表中没有找到对应的维度数据:" + selectSql);
            }
        }

        //关闭Jedis连接
        if (jedis != null) {
            System.out.println("~~关闭Jedis客户端~~");
            jedis.close();
        }

        return dimJsonObj;
    }


    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接查询语句 select * from dim_base_trademark where id ='12'
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='" + columnValue + "' ");
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
            }
        }
        System.out.println("从Phoenix表中查询数据的SQL:" + selectSql);

        List<JSONObject> jsonObjList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
        JSONObject jsonObj = null;
        if (jsonObjList != null && jsonObjList.size() > 0) {
            jsonObj = jsonObjList.get(0);
        } else {
            System.out.println("在维度表中没有找到对应的维度数据:" + selectSql);
        }
        return jsonObj;
    }

    public static void main(String[] args) {
        //JSONObject dimInfo = getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "17"), Tuple2.of("tm_name", "ddd"));
        //JSONObject dimInfo = getDimInfo("dim_base_trademark", Tuple2.of("id", "17"));
        JSONObject dimInfo = getDimInfo("dim_base_trademark", "17");
        System.out.println(dimInfo);
    }

    public static void delCached(String tableName, String key) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + key;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
