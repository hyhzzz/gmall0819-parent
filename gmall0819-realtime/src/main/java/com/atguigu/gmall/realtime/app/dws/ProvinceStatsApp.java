package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-28 20:10
 * 地区主题统计
 */
public class ProvinceStatsApp {
    public static void main(String[] args) throws Exception {

        //1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.检查点相关设置
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
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/ProvinceStatsApp"));

        //2.7 设置操作hdfs用户权限
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //3.从kafka主题中读取数据，创建动态表
        String topic = "dwm_order_wide";
        String groupId = "province_stats_group";

        tableEnv.executeSql("CREATE TABLE order_wide (" +
                "province_id BIGINT," +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id BIGINT," +
                "split_total_amount DECIMAL(16, 2)," +
                "create_time STRING," +
                "rowtime as TO_TIMESTAMP(create_time)," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND " +
                ") WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //4.从动态表中查询数据
        Table tableRes = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND) ,'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND) ,'yyyy-MM-dd HH:mm:ss') edt," +
                "province_id," +
                "    province_name," +
                "    province_area_code area_code," +
                "    province_iso_code iso_code," +
                "    province_3166_2_code iso_3166_2," +
                "    count(distinct order_id) order_count," +
                "    sum(split_total_amount) order_amount," +
                "    UNIX_TIMESTAMP()*1000 ts" +
                " from order_wide group by TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                " province_id,province_name,province_area_code, province_iso_code ,province_3166_2_code");

        //5.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(tableRes, ProvinceStats.class);

        //6.将流中的数据写入到clickhouse中
        provinceStatsDS.print(">>>>>");

        provinceStatsDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into  province_stats_0819 values(?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
