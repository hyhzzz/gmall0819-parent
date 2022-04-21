package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-18 23:17
 * 自定义flinkcdc反序列
 */
public class MySchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");

        //获取数据库名
        String database = sourceStruct.getString("db");
        //获取表名
        String table = sourceStruct.getString("table");
        //获取操作类型
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //拼接json对象
        JSONObject resJsonObj = new JSONObject();
        resJsonObj.put("database", database);
        resJsonObj.put("table", table);
        resJsonObj.put("type", type);

        JSONObject dataJsonObj = new JSONObject();
        Struct afterStruct = valueStruct.getStruct("after");
        if (afterStruct != null) {
            List<Field> fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                dataJsonObj.put(fieldName, fieldValue);
            }
        }
        resJsonObj.put("data", dataJsonObj);

        collector.collect(resJsonObj.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}