package com.swenchao.apitest.transform;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * @author: Swenchao
 * @description: 富函数使用
 * @create: 2021-02-20 18:34
 **/
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 读取数据进行转换
        DataStreamSource<String> sensorDataStreamSource = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> sensorDataStream = sensorDataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] details = s.split(",");
                return new SensorReading(details[0], Long.valueOf(details[1]), Double.valueOf(details[2]));
            }
        });
        DataStream<Tuple2<String, Integer>> resultStream = sensorDataStream.map(new MyMapFunction());
        resultStream.print();
        env.execute();
    }

    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        // 进行初始化操作
        @Override
        public void open(Configuration configuration) throws Exception {
            System.out.println("open");
        }

        // 进行一些结束操作
        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
