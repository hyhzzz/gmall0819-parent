package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author coderhyh
 * @create 2022-04-21 0:13
 */
public class UniqueVisitorApp {
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
        String topic = "dwd_page_log";
        String groupId = "unique_visitor_app_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaSource);

        //4.对读取的数据进行类型转换  json字符串-->json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //jsonObjDS.print(">>");

        //5.过滤
        //5.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //5.2 使用flink状态编程，完成数据过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(
                new RichFilterFunction<JSONObject>() {
                    private ValueState<String> lastVisitDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.days(1))
                                //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //获取当前数据的lastPageId
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        //判断是否从其他页面跳转过来的，如果是直接跳出
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }

                        //从状态中获取上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //把时间戳转换为时间字符串
                        String curVisitDate = sdf.format(jsonObj.getLong("ts"));
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && curVisitDate.equals(lastVisitDate)) {
                            //曾经访问过  这次访问不算是独立访客  需要过滤掉
                            return false;
                        } else {
                            //以前没有访问过   将这个访问的日期放到状态中
                            lastVisitDateState.update(curVisitDate);
                            return true;
                        }
                    }
                }
        );

        filterDS.print(">>>");

        // 6.将过滤后的数据写到kafka的dwm_unique_visitor主题中
        filterDS
                .map(jsonObj -> jsonObj.toJSONString())
                .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visitor"));

        env.execute();
    }
}
