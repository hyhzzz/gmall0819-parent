package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author coderhyh
 * @create 2022-04-19 9:02
 * 业务数据的动态分流逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> dimTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection conn;

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //处理主流数据
    //根据当前处理数据的table和type拼接key，到状态中获取对应的配置tableprocess对象根据配置对象进行分流
    @Override
    public void processElement(JSONObject jsonObj,
                               BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> collector) throws Exception {


        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //获取业务操作的业务数据表名
        String table = jsonObj.getString("table");
        //获取对业务数据库表的操作类型
        String type = jsonObj.getString("type");
        //注意：如果使用maxwell读取历史数据，操作类型是bootstrap-insert,需要进行修正
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //拼接从状态中获取配置信息的key
        String key = table + ":" + type;

        //根据key从广播状态中获取当前操作所对应的配置信息
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //不管是事实数据还是维度数据，在向下游传递前，都应该携带上目的地属性
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table", sinkTable);

            //对不需要的属性进行过滤
            filterColumn(jsonObj.getJSONObject("data"),tableProcess.getSinkColumns());

            //判断是事实还是维度
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //维度数据---放到维度侧输出流中
                ctx.output(dimTag, jsonObj);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //事实数据---放到主流中
                collector.collect(jsonObj);
            }
        } else {
            System.out.println("No this key in tableProcess:" + key);
        }
    }


    //处理广播流数据
    //读取配置信息，封装为tableprocess对象，放到广播状态中
    @Override
    public void processBroadcastElement(String jsonStr,
                                        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context,
                                        Collector<JSONObject> collector) throws Exception {

        //将反序列化后得到json字符串转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取配置表中的一条配置信息
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        //将配置信息封装为一个TableProcess对象
        TableProcess tableProcess = dataJsonObj.toJavaObject(TableProcess.class);

        //获取业务数据库表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //获取输出类型    hbase   kafka
        String sinkType = tableProcess.getSinkType();
        //获取输出目的地
        String sinkTable = tableProcess.getSinkTable();
        //获取建表字段---保留字段
        String sinkColumns = tableProcess.getSinkColumns();
        //获取建表主键
        String sinkPk = tableProcess.getSinkPk();
        //获取建表扩展
        String sinkExtend = tableProcess.getSinkExtend();

        //拼接放到广播状态中的配置的key
        String key = sourceTable + ":" + operateType;

        //判断读取到的配置是不是维度配置  如果是维度配置  提前将维度表创建出来
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        //将配置信息放到广播状态中
        broadcastState.put(key, tableProcess);

    }

    /**
     * 字段过滤
     * @param dataJsonObj 要过滤的json对象  "data":{"tm_name":"xzls","logo_url":"fd","id":13}
     * @param sinkColumns  保留的字段        id,tm_name
     */
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        //asList：数组转集合
        List<String> columnList = Arrays.asList(columnArr);
        //对过滤的json对象进行遍历
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }


    //提前创建维度表
    private void checkTable(String tableName, String fields, String pk, String ext) {

        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }

        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");

        String[] fieldArr = fields.split(",");
        for (int i = 0; i < fieldArr.length; i++) {
            String fieldName = fieldArr[i];
            //判断是否为主键
            if (fieldName.equals(pk)) {
                createSql.append(fieldName + "  varchar primary key");
            } else {
                createSql.append(fieldName + "  varchar");
            }
            if (i < fieldArr.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")" + ext);
        System.out.println("phoenix中的建表语句是：" + createSql.toString());

        PreparedStatement ps = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在phoenix中建表失败~~");
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
