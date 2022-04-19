package com.atguigu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-17 20:34
 * 使用flinkcdc读取mysql表中的数据 api方式
 */
public  class FlinkCDC_01_Api {
   public static void main(String[] args) throws Exception {

      //1.创建执行环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
      //2.1 开启Checkpoint,每隔5秒钟做一次CK
      //env.enableCheckpointing(5000L);
      //2.2 指定CK的一致性语义
      //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      //2.3 设置任务关闭的时候保留最后一次CK数据
      //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
      //2.4 指定从CK自动重启策略
      //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
      //2.5 设置状态后端
      //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
      //2.6 设置访问HDFS的用户名
      //System.setProperty("HADOOP_USER_NAME", "atguigu");

      //3.创建Flink-MySQL-CDC的Source
      //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
      //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
      //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
      //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
      DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
              .hostname("hadoop102")
              .port(3306)
              .username("root")
              .password("123456")
              //监控这台数据库服务器上的哪些数据库的变化数据，如果不配的话，就是监控这台数据库的所有
              .databaseList("gmall0819_realtime")
              //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
              //监控哪几张表  库名.表名 如果不加库名就等于读取当前库中所有的数据
              .tableList("gmall0819_realtime.t_user")
              //在第一次启动的时候，对监控的数据库表执行初始快照，读取最新的binglog
              //latest :不做快照，直接切到binglog最新位置开始读取
              .startupOptions(StartupOptions.initial())
              //序列化方式
              .deserializer(new StringDebeziumDeserializationSchema())
              .build();

      //4.使用CDC Source从MySQL读取数据
      DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

      //5.打印数据
      mysqlDS.print();

      //6.执行任务
      env.execute();
   }
}
