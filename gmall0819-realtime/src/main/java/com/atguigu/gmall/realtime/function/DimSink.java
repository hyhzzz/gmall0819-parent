package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @author coderhyh
 * @create 2022-04-20 9:54
 * 将维度侧输出流中的数据写到phoenix表中
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {

        //获取输出目的地表名
        String sinkTable = jsonObj.getString("sink_table");

        //获取要输出到维度表中的数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //拼接upsert语句
        String upsertSql = genUpsertSql(sinkTable, dataJsonObj);
        System.out.println("向Phoenix表中插入数据的SQL：" + upsertSql);

        //获取数据库操作对象
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(upsertSql);
            //执行SQL语句
            ps.execute();
            //注意：Phoenix事务默认是手动提交
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("向phoenix表中插入数据发生了异常");
        } finally {
            //释放资源
            if (ps != null) {
                ps.close();
            }
        }

        //如果维度发生了修改，那么需要将redis中缓存为缓存的改维度数据删除掉
        if ("update".equals(jsonObj.getString("type"))) {
            DimUtil.delCached(sinkTable, dataJsonObj.getString("id"));
        }

    }

    //拼接向phoenix表中插入数据的sql dataJsonObj:{"tm_name":"lzls","id":14}
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();

        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + tableName
                + " (" + StringUtils.join(keys, ",")
                + ") values ('" + StringUtils.join(values, "','") + "')";
        return upsertSql;

    }
}
