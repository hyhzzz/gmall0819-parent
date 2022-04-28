package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author coderhyh
 * @create 2022-04-27 23:03
 * 操作ClickHouse的工具类
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                //"insert into 表 values(?,?,?,?,?)",
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //给问号占位符赋值
                        //获取流中对象所属类型中的属性
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < fieldArr.length; i++) {
                            Field field = fieldArr[i];

                            //判断当前属性是否被@TransientSink注解修饰
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }

                            //设置私有属性访问权限
                            field.setAccessible(true);
                            try {
                                //获取属性的值
                                Object fieldValue = field.get(obj);
                                //将对象属性的值赋值给问号占位符
                                ps.setObject(i + 1 - skipNum, fieldValue);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        //批量处理大小
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl("jdbc:clickhouse://hadoop102:8123/default")
                        .build()
        );
        return sinkFunction;
    }
}
