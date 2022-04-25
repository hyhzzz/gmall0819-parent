package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.function.DimSink;
import com.atguigu.gmall.realtime.function.MySchema;
import com.atguigu.gmall.realtime.function.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author coderhyh
 * @create 2022-04-17 10:42
 * 从Kafka中读取ods层业务数据 并进行处理  发送到DWD层
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1. 基本环境准备
        //1.1 指定流处理环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度 和kafka分区数一致
        env.setParallelism(4);

        //2. 检查点相关设置
        //2.1 开启检查点 每5秒做一次ck
        //env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //2.2 设置检查点超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.3 设置job取消后检查点是否保留
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2.4 设置检查点最小时间间隔
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //2.5 设置检查点重启策略
        //失败率重启
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //过30天 重启次数清0
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.milliseconds(30000L), Time.days(30)));

        //2.6 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/BaseDBApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaSource);

        //4.对读取的数据进行类型转换  json字符串-->json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //5. 进行简单etl数据清洗
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                boolean flag =
                        jsonObj.getString("table") != null
                                && jsonObj.getString("table").length() > 0
                                && jsonObj.getJSONObject("data") != null
                                && jsonObj.getString("data").length() > 3;


                return flag;
            }
        });

        //filterDS.print(">>");


        //6. 使用flinkcdc读取配置表数据 得到一个配置流   配置表在mysql数据库中,使用flink-sql-cdc来实时监控配置表数据
        //6.1创建mysqlsource
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                //监控这台数据库服务器上的哪些数据库的变化数据，如果不配的话，就是监控这台数据库的所有
                .databaseList("gmall0819_realtime")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                //监控哪几张表  库名.表名 如果不加库名就等于读取当前库中所有的数据
                .tableList("gmall0819_realtime.table_process")
                //在第一次启动的时候，对监控的数据库表执行初始快照，读取最新的binglog
                //latest :不做快照，直接切到binglog最新位置开始读取
                .startupOptions(StartupOptions.initial())
                //序列化方式
                .deserializer(new MySchema())
                .build();

        //6.2 从mysql中读取配置信息 得到配置流
        DataStreamSource<String> mySQLStrDS = env.addSource(mysqlSource);

        //7.把配置流变成广播流，并定义广播状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mySQLStrDS.broadcast(mapStateDescriptor);

        //8.将主流(业务流)和广播流(配置流)进行连接
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcast);

        //9.对连接后的数据进行处理  动态分流  事实-主流  维度-侧输出流
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };

        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
                new TableProcessFunction(dimTag, mapStateDescriptor));

        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        dimDS.print("维度数据");
        realDS.print("事实数据");

        //10 将维度侧输出流数据写到phoenix不同表中
        dimDS.addSink(new DimSink());

        //11 将主流事实数据写回到不同kafka dwd主题中
        realDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {

                        String topic = jsonObj.getString("sink_table");
                        return new ProducerRecord<byte[], byte[]>(
                                topic,
                                jsonObj.getJSONObject("data").toJSONString().getBytes());
                    }
                }
        ));

        env.execute();
    }
}
