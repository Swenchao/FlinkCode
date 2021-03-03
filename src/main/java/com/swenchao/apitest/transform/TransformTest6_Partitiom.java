package com.swenchao.apitest.transform;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Swenchao
 * @description: 重分区
 * @create: 2021-03-03 09:59
 **/
public class TransformTest6_Partitiom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");
        inputStream.print("input");

        // shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // KeyBY
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] details = s.split(",");
                return new SensorReading(details[0], Long.valueOf(details[1]), Double.valueOf(details[2]));
            }
        });
        dataStream.keyBy("id").print("keyBy");

        env.execute();
    }
}
