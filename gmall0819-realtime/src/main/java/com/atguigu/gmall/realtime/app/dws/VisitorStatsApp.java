package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author coderhyh
 * @create 2022-04-27 18:50
 * 访客主题统计
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/VisitorStatsApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");


        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String pageLogTopic = "dwd_page_log";
        String uvTopic = "dwm_unique_visitor";
        String ujdTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageLogTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uvTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(ujdTopic, groupId);

        //3.3 消费数据封装为流
        DataStreamSource<String> pageLogStrDS = env.addSource(pageViewSource);
        DataStreamSource<String> uvStrDS = env.addSource(uniqueVisitSource);
        DataStreamSource<String> ujdStrDS = env.addSource(userJumpSource);

        //pageLogStrDS.print(">>>>");
        //uvStrDS.print("####");
        //ujdStrDS.print("&&&");

        //4.对不同流中的数据进行类型转换，json字符串-->实体类对象
        //4.1 页面日志---pv、sv、dur
        SingleOutputStreamOperator<VisitorStats> pageLogStatsDS = pageLogStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L, 1L, 0L, 0L,
                                pageJsonObj.getLong("during_time"),
                                jsonObj.getLong("ts")
                        );
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            visitorStats.setSv_ct(1L);
                        }
                        return visitorStats;
                    }
                }
        );

        //4.2 独立访客---uv
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                1L, 0L, 0L, 0L, 0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );


        //4.3 用户跳出---ujd
        SingleOutputStreamOperator<VisitorStats> ujdStatsDS = ujdStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L, 0L, 0L, 1L, 0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        //5. 使用union进行合并
        DataStream<VisitorStats> unionDS = pageLogStatsDS
                .union(uvStatsDS, ujdStatsDS);

        //6.设置watermark以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                                        return visitorStats.getTs();
                                    }
                                }
                        )
        );

        //7.分组 注意：不要按照mid进行分组，因为单位窗口内，不会有太多数据产生，起不到聚合效果
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getVc(),
                                visitorStats.getCh(),
                                visitorStats.getAr(),
                                visitorStats.getIs_new()
                        );
                    }
                }
        );

        //8.开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //9 聚合计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());

                        return stats1;
                    }
                },
                //对聚合之后的数据进行处理（补充窗口时间属性）
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> tuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        for (VisitorStats visitorStats : input) {
                            visitorStats.setStt(DateTimeUtil.toYMDHMS(new Date(window.getStart())));
                            visitorStats.setEdt(DateTimeUtil.toYMDHMS(new Date(window.getEnd())));
                            visitorStats.setTs(System.currentTimeMillis());
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        reduceDS.print(">>>>>");

        //10 将聚合计算的结果写到clickhouse数据库中
        reduceDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into visitor_stats_0819 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
