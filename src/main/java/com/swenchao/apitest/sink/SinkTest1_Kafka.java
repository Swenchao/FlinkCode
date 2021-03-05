package com.swenchao.apitest.sink;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author: Swenchao
 * @description: kafka连接
 * @create: 2021-03-03 13:54
 **/
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据进行转换

        // 从文件读取数据
//        DataStreamSource<String> sensorDataStreamSource = env.readTextFile("src/main/resources/sensor.txt");

        // 从Kafka读取数据
        // 参数设置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 下面参数不重要
        properties.setProperty("group.id", "consumer" + "-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization" + ".StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common" + ".serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> sensorDataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("sensor",
                new SimpleStringSchema(), properties));

        // 直接将类转换成字符串，避免下面再进行序列化
        SingleOutputStreamOperator<String> sensorDataStream = sensorDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] details = s.split(",");
                // 封装成字符串
                return new SensorReading(details[0], Long.valueOf(details[1]), Double.valueOf(details[2])).toString();
            }
        });

        // sink输出到kafka
        sensorDataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "flinkTest",
                new SimpleStringSchema()));

        env.execute();
    }
}
