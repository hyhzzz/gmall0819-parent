package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author coderhyh
 * @create 2022-04-22 12:49
 * 订单宽表的计算
 * 处理订单和订单明细数据形成订单宽表
 */
public class OrderWideApp {
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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/OrderWideApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");


        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String orderInfoTopic = "dwd_order_info";
        String orderDetailTopic = "dwd_order_detail";
        String groupId = "order_wide_app_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(orderDetailTopic, groupId);

        //3.3 消费数据 封装为流
        DataStreamSource<String> orderInfoStrDS = env.addSource(orderInfoKafkaSource);
        DataStreamSource<String> orderDetailStrDS = env.addSource(orderDetailKafkaSource);

        //orderInfoStrDS.print(">>");
        //orderDetailStrDS.print("##");

        //4. 对读取的数据进行类型转换  json字符串-->实体类对象
        //4.1 订单
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );


        //4.2 订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

        //5. 指定watermark以及提取事件时间字段
        //5.1 订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                        return orderInfo.getCreate_ts();
                                    }
                                }
                        )
        );
        //5.2 订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderDetail>() {
                                            @Override
                                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                                return orderDetail.getCreate_ts();
                                            }
                                        }
                                )
                );

        //6. 使用keyby进行分组---指定两条流的连接字段
        //6.1 订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(OrderInfo::getId);
        //6.2 订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(OrderDetail::getOrder_id);

        //7. 订单和订单明细流的双流join
        SingleOutputStreamOperator<OrderWide> joinedDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo,
                                               OrderDetail orderDetail,
                                               ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context,
                                               Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        joinedDS.print(">>>>>");

        //8.和用户维度进行关联
        //将异步I/O操作应用于DataStream作为DataStream的一次转换操作
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(
                joinedDS,
                new DimAsyncFunction<OrderWide>("dim_user_info") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject userInfoJsonObj) throws Exception {
                        orderWide.setUser_gender(userInfoJsonObj.getString("GENDER"));
                        //1985-02-19
                        String birthday = userInfoJsonObj.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Long birthdayTime = sdf.parse(birthday).getTime();
                        Long curTime = System.currentTimeMillis();
                        Long ageLong
                                = (curTime - birthdayTime) / 1000L / 60L / 60L / 24L / 365L;
                        orderWide.setUser_age(ageLong.intValue());
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //orderWideWithUserInfoDS.print(">>>>");
        //9.和地区维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserInfoDS,
                new DimAsyncFunction<OrderWide>("dim_base_province") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject provinceJsonObj) throws Exception {
                        orderWide.setProvince_name(provinceJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(provinceJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(provinceJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(provinceJsonObj.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );

        //orderWideWithProvinceDS.print(">>>");
        //10.和商品维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuInfoDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("dim_sku_info") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject skuInfoJsonObj) throws Exception {
                        orderWide.setSku_name(skuInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(skuInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(skuInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(skuInfoJsonObj.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );
        //orderWideWithSkuInfoDS.print(">>>");
        //11.和SPU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSkuInfoDS,
                        new DimAsyncFunction<OrderWide>("dim_spu_info") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject spuInfoJsonObj) throws Exception {
                                orderWide.setSpu_name(spuInfoJsonObj.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return orderWide.getSpu_id().toString();
                            }
                        },
                        60, TimeUnit.SECONDS
                );
        //12.和类别维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //13.和品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithTmDS.print(">>>>>>");

        //14.将关联后的订单宽表数据写到kafka的dwm_order_wide主题
        orderWideWithTmDS
                .map(orderWide->JSON.toJSONString(orderWide))
                .addSink(MyKafkaUtil.getKafkaSink("dwm_order_wide"));

        env.execute();
    }
}
