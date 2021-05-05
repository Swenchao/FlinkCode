package com.swenchao.apitest.window;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Author: swenchao
 * Description: 计数窗口函数使用
 * Date 2021/4/21 9:04 下午
 * Version: 1.0
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // socket文本流
        DataStreamSource<String> sensorDataStreamSource = env.socketTextStream("localhost", 7777);

        // 转换成 SensorReading 类型
        SingleOutputStreamOperator<SensorReading> mapStream = sensorDataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fileds = s.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
            }
        });

        // 开计数窗口测试（）
//        sensorDataStreamSource.keyBy("id")
                // 10个数一个窗口，隔两个数开一个窗口
//                .countWindow(10, 2)
//                .aggregate()

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> accu) {
            return new Tuple2<>(accu.f0 + sensorReading.getTemperature(), accu.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accu) {
            return accu.f0 / accu.f1 * 1.0;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
