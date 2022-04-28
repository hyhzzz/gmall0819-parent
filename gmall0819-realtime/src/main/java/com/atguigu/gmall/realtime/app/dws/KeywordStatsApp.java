package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.KeywordUDTF;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-29 0:06
 * 关键词主题统计
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度 和kafka分区数一致
        env.setParallelism(4);

        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //3.从kafka主题中读取数据 创建动态表
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_group";

        tableEnv.executeSql("create table page_log(" +
                "common MAP<STRING, STRING>," +
                "page  MAP<STRING, STRING>," +
                "ts BIGINT," +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
                ") WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        // 4.从动态表中过滤搜索行为
        Table fullwordTable = tableEnv.sqlQuery("select page['item'] fullword,rowtime from  page_log where " +
                "page['page_id']='good_list' and page['item'] is not null");

        //5.对查询的结果进行分词，并和原表进行连接
        Table keywordTable = tableEnv.sqlQuery("SELECT keyword,rowtime FROM " + fullwordTable + ", LATERAL TABLE(ik_analyze(fullword)) AS T(keyword)");

        // 6.分组、开窗、聚合计算
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + keywordTable
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");


        //7.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);


        // 8.将流中的数据写到Clickhouse中
        keywordStatsDS.print(">>>>");

        keywordStatsDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into keyword_stats_0819(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );


        env.execute();
    }
}
