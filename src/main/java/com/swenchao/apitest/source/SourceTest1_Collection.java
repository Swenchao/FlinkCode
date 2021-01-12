package com.swenchao.apitest.source;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度是1，保证读取顺序跟存的时候顺序一样
        env.setParallelism(1);

        // 从集合中读取数据
        List<SensorReading> dataStream = Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1));
        DataStream<SensorReading> dataStreamSource = env.fromCollection(dataStream);

        dataStreamSource.print("data");

        env.execute();
    }
}
