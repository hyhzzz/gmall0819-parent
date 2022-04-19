package com.atguigu.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-18 0:21
 * 自定义序列化方式
 */
public class FlinkCDC_03_CustomSchema {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        //env.enableCheckpointing(5000L);
        //2.2 指定CK的一致性语义
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/FlinkCDC_03_CustomSchema"));
        //2.6 设置访问HDFS的用户名
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //3.创建Flink-MySQL-CDC的Source
        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                //监控这台数据库服务器上的哪些数据库的变化数据，如果不配的话，就是监控这台数据库的所有
                .databaseList("gmall0819_realtime")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                //监控哪几张表  库名.表名 如果不加库名就等于读取当前库中所有的数据
                .tableList("gmall0819_realtime.t_user")
                //在第一次启动的时候，对监控的数据库表执行初始快照，读取最新的binglog
                //latest :不做快照，直接切到binglog最新位置开始读取
                .startupOptions(StartupOptions.initial())
                //序列化方式
                .deserializer(new MySchema())
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        //5.打印数据
        mysqlDS.print();

        //6.执行任务
        env.execute();
    }

    public static class MySchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {


            Struct valueStruct = (Struct) sourceRecord.value();
            Struct sourceStruct = valueStruct.getStruct("source");


            //获取数据库名称
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
            if (afterStruct!=null){
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
}
