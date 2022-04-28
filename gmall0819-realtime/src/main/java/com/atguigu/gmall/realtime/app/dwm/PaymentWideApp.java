package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-27 1:12
 * 支付宽表数据
 */
public class PaymentWideApp {
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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/PaymentWideApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");


        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String paymentInfoTopic = "dwd_payment_info";
        String orderWideTopic = "dwm_order_wide";
        String groupId = "payment_wide_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideTopic, groupId);

        //3.3 消费数据  封装为流
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoSource);
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideSource);

        //paymentInfoStrDS.print(">>>");
        //orderWideStrDS.print("###");

        //4.对读取的数据进行类型转换   jsonStr->实体类对象
        //4.1 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String jsonStr) throws Exception {
                PaymentInfo paymentInfo = JSON.parseObject(jsonStr, PaymentInfo.class);
                return paymentInfo;
            }
        });


        //4.2 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(
                new MapFunction<String, OrderWide>() {
                    @Override
                    public OrderWide map(String jsonStr) throws Exception {
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        return orderWide;
                    }
                }
        );

        //5.设置watermark以及提取事件时间字段
        //5.1 支付流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );

        //5.2 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );

        //6.使用keyby进行分组 指定两条流的连接字段
        //6.1 支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);

        //6.2 订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //7.使用intervaljoin进行双流join
        SingleOutputStreamOperator<PaymentWide> joinedDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        joinedDS.print(">>>>");

        //8.将join的结果写回到kafka对应的主题中 dwm_paymentr_wider
        joinedDS
                .map(paymentWide -> JSON.toJSONString(paymentWide))
                .addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));
        env.execute();

    }
}
