package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-21 17:55
 * 用户访客跳出明细计算
 */
public class UserJumpDetailApp {
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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/UserJumpDetailApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");


        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaSource);

       /* DataStream<String> kafkaStrDS = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":15000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":30000} "
                );*/


        //4.对读取的数据进行类型转换  json字符串-->json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //jsonObjDS.print(">>");

        //5.指定watermark 以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonOb, long l) {
                                return jsonOb.getLong("ts");
                            }
                        })
        );

        //6.使用mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //7.使用flinkcep 进行数据过滤
        //7.1 定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10));
        //7.2 将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        //7.3 从流中去提取匹配的数据
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOutTag") {
        };

        SingleOutputStreamOperator<String> filterDS = patternDS.flatSelect(
                timeOutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //从流中提取和pattern匹配的超时数据
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObj : jsonObjectList) {
                            //注意：这里虽然使用out.collect向下游传递数据，其实是将数据放到参数1声明的侧输出流中
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //从流中提取和pattern完全匹配的数据  属于跳转 ，我们要计算跳出明细 ，所以这里我们不需要处理
                    }
                }
        );


        //注意：超时数据是放到侧输出流中的
        DataStream<String> timeoutDS = filterDS.getSideOutput(timeOutTag);
        timeoutDS.print(">>>>");

        //8.将过滤后的数据写回kafka dwm主题中 dwm_user_jump_detail
        timeoutDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        env.execute();
    }
}
