package com.swenchao.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author : swenchao
 * create at:  2021/1/12  7:34 下午
 * @description: 从 Kafka 中读取数据
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从Kafka读取数据

        // 参数设置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 下面不重要
        properties.setProperty("group.id", "consumer" + "-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization" + ".StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common" + ".serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor",
                new SimpleStringSchema(), properties));

        // 打印
        dataStream.print();
        env.execute();
    }
}