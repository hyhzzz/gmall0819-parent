package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author coderhyh
 * @create 2022-04-16 18:49
 * 操作Kafka的工具类
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);

        return kafkaConsumer;
    }
}
