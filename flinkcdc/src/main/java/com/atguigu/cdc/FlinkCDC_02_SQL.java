package com.atguigu.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-18 0:16
 */
class FlinkCDC_02_SQL {
    public static void main(String[] args) throws Exception {

        //1.准备环境
        //1.1流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.创建动态表
        tableEnv.executeSql("CREATE TABLE user_info (" +
                "  id INT," +
                "  name STRING," +
                "  age INT" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'hadoop102'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                "  'database-name' = 'gmall0819_realtime'," +
                "  'table-name' = 't_user'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();


        // 6.执行任务
        env.execute();

    }
}
