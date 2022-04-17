package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author coderhyh
 * @create 2022-04-16 19:03
 * 从Kafka中读取ods层用户行为日志数据进行日志数据分流
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        //1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度 和kafka分区数一致
        env.setParallelism(4);

        //2. 检查点相关设置
        //2.1 开启检查点 每5秒做一次ck
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2.4 设置检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //2.5 设置检查点重启策略
        //失败率重启
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //过30天 重启次数清0
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.milliseconds(30000L), Time.days(30)));

        //2.6 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/BaseLogApp"));

        //2.7 设置操作hdfs用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //3.从kafka中读取数据
        //3.1 定义消费的主题以及消费者组
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        //3.2获取消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaSource);

        //4. 对读取的数据进行类型转换 json字符串-->json对象
        //4.1 匿名内部类
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                }
        );
        //4.2 lambda表达式
        //kafkaStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        //4.3 方法引用
        //kafkaStrDS.map(JSON::parseObject);

        //jsonObjDS.print(">>");

        //5.对新老访客标记进行修复
        //5.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //5.2 使用flink的状态，判断是否曾经访问过，对标记进行修复
        //修复思路：将当前设备第一次访问日期放到状态中存储起来，下次在进行访问的时候，先从状态中获取访问日期，
        //如果获取到说明曾经访问过，将标记修复为0，如果从状态中没有获取到数据，说明是第一次访问，将本次访问日期放到状态中
        SingleOutputStreamOperator<JSONObject> jsonObjWithIsNewDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateSate;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        lastVisitDateSate = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDateSate", String.class));
                        sdf = new SimpleDateFormat("yyyyMMdd");

                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {

                        //获取当前日志中新老访客标记
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");

                        //如果新老访客标记时候1的话，有可能需要进行修复
                        if ("1".equals(isNew)) {
                            //从状态中获取上次访问日期
                            String lastVisitDate = lastVisitDateSate.value();
                            //获取当前访问日期  format:把Date日期转成字符串
                            String curVisitDate = sdf.format(jsonObj.getLong("ts"));

                            //判断是否曾经访问过
                            if (lastVisitDate != null && lastVisitDate.length() > 0) {
                                //曾经访问过 对状态标记进行修复
                                if (!curVisitDate.equals(lastVisitDate)) {
                                    jsonObj.getJSONObject("common").put("is_new", "0");
                                }
                            } else {
                                //第一次访问，将这次访问日期放到状态中

                                lastVisitDateSate.update(curVisitDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );

        //jsonObjWithIsNewDS.print(">>>");

        //6.使用侧输出流对日志数据进行分流处理   启动---侧输出流  曝光-->侧输出流  页面-->主流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };

        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithIsNewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj,
                                       ProcessFunction<JSONObject, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                //判断是否为启动日志
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    //启动日志
                    ctx.output(startTag, startJsonObj.toJSONString());
                } else {
                    //除了启动日志之外 其他都属于页面日志
                    out.collect(jsonObj.toJSONString());
                    //判断在页面上是否有曝光信息
                    JSONArray displaysArr = jsonObj.getJSONArray("displays");
                    if (displaysArr != null && displaysArr.size() > 0) {
                        //如果有曝光信息，对曝光数据进行遍历
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        Long ts = jsonObj.getLong("ts");

                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject displaysJsonObj = displaysArr.getJSONObject(i);

                            displaysJsonObj.put("page_id", pageId);
                            displaysJsonObj.put("ts", ts);
                            ctx.output(displayTag, displaysJsonObj.toJSONString());
                        }
                    }
                }

            }
        });

        pageDS.print("page");
        pageDS.getSideOutput(startTag).print("start");
        pageDS.getSideOutput(displayTag).print("display");

        //7.将不同流的数据 写入到kafka不同主题中


        //8.执行任务
        env.execute();

    }
}
