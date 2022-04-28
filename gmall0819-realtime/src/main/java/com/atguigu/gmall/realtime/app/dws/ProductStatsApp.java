package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author coderhyh
 * @create 2022-04-28 12:04
 * 商品主题统计
 */
public class ProductStatsApp {
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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/ProductStatsApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");


        //3.从kafka主题中读取数据
        //3.1 声明消费者主题和消费者组
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        //3.3 消费数据封装为流
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //4.对读取的数据进行类型转换  json字符串-->实体类对象
        //4.1 点击和曝光
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pageViewDStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");

                        //判断是否是商品的点击行为： pageId==good_detail
                        if ("good_detail".equals(pageJsonObj.getString("page_id"))) {
                            //如果是点击行为，将当前点击行为封装为ProductStats对象
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(pageJsonObj.getLong("item"))
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }
                        //判断当前页面上是否有曝光行为
                        JSONArray displaysArr = jsonObj.getJSONArray("displays");
                        if (displaysArr != null && displaysArr.size() > 0) {
                            for (int i = 0; i < displaysArr.size(); i++) {
                                JSONObject displaysJsonObj = displaysArr.getJSONObject(i);
                                //判断曝光的是不是商品
                                if ("sku_id".equals(displaysJsonObj.getString("item_type"))) {
                                    //将当前商品的曝光行为 封装为ProductStats对象
                                    ProductStats productStats = ProductStats.builder()
                                            .sku_id(displaysJsonObj.getLong("item"))
                                            .ts(ts)
                                            .display_ct(1L)
                                            .build();
                                    out.collect(productStats);
                                }

                            }
                        }
                    }
                }
        );


        //4.2 收藏
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                                .favor_ct(1L)
                                .build();
                    }
                }
        );


        //4.3 加购
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                                .cart_ct(1L)
                                .build();
                    }
                }
        );


        //4.4 下单
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);

                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                                .build();
                    }
                }
        );


        //4.5 支付
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String json) throws Exception {
                        PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                        Long ts = DateTimeUtil.toTs(paymentWide.getCallback_time());
                        return ProductStats.builder().sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .ts(ts).build();
                    }
                });

        //4.6 退单
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;
                });

        //4.7 评论
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDStream.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                });


        //5. 使用union进行合并
        DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
                favorInfoStatsDS,
                cartInfoStatsDS,
                orderWideStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );

        //unionDS.print(">>>>>");

        //6. 指定watermark以及提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );
        //7. 分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id);

        //8. 开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //9. 聚合计算
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                },
                new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        String stt = DateTimeUtil.toYMDHMS(new Date(window.getStart()));
                        String edt = DateTimeUtil.toYMDHMS(new Date(window.getEnd()));
                        for (ProductStats productStats : input) {
                            productStats.setStt(stt);
                            productStats.setEdt(edt);
                            productStats.setTs(System.currentTimeMillis());
                            out.collect(productStats);
                        }
                    }
                }
        );


        // 10.和商品SKU维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("dim_sku_info") {
                    @Override
                    public void join(ProductStats productStats, JSONObject skuInfoJsonObj) throws Exception {
                        productStats.setSku_name(skuInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(skuInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(skuInfoJsonObj.getLong("SPU_ID"));
                        productStats.setCategory3_id(skuInfoJsonObj.getLong("CATEGORY3_ID"));
                        productStats.setTm_id(skuInfoJsonObj.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );

        //11.和商品SPU维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(
                        productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //12.和商品类别维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //13.和商品品牌维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print(">>>>>>");

        //14.将聚合结果写到clickhouse中
        productStatsWithTmDS.addSink(
                ClickHouseUtil.<ProductStats>getSinkFunction("insert into product_stats_0819 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
