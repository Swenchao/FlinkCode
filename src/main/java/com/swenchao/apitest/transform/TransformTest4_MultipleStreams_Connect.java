package com.swenchao.apitest.transform;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author : swenchao
 * create at:  2021/2/17  4:21 下午
 * @description: 多流转换操作（两流合并）
 */
public class TransformTest4_MultipleStreams_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fileds = s.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
            }
        });

        // lambda表达式写法
//        SingleOutputStreamOperator<SensorReading> mapLambdaStream = dataStream.map(line -> {
//            String[] fileds = line.split(",");
//            return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
//        });

        // 分组（按照温度值30为界限来区分为高温和低温）
        SplitStream<SensorReading> splitStream = mapStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 30) ? Collections.singletonList("high") :
                        Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

//        highTempStream.print("high");
//        lowTempStream.print("low");
//        allTempStream.print("all");

        //  将高低温流合并，高温传感器输出 "high warning"警告，低温流输出 "normal"

        // 可直接封装进行返回，也可使用下面的方法先包装高温，再结合返回

        // 将高低温流合并
        ConnectedStreams<SensorReading, SensorReading> allConnect = highTempStream.connect(lowTempStream);

        // 使用CoMap进行转换
        // 其中CoMapFunction方法中有两个SensorReading，第一个为原始高温流，第二个为原始低温流（因为是以高温流为基础进行构建的合成流，后来又加入的低温流）
        // 查看CoMapFunction源码注解可得：CoMapFunction中map1 map2分别对应的connected streams中第一个元素和第二个元素。最终两个map返回相同的类型
        SingleOutputStreamOperator<Object> resultMap = allConnect.map(new CoMapFunction<SensorReading, SensorReading, Object>() {
            @Override
            public Object map1(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(), "High Warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(), "Normal");
            }
        });

        // 第二种写法

        // 现将高温流进行包装（二元祖）
//        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream =
//                highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
//                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
//            }
//        });
//
        // 高低温进行组合
//        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(lowTempStream);
//        SingleOutputStreamOperator<Object> resultMap = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
//            @Override
//            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
//                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high warning");
//            }
//
//            @Override
//            public Object map2(SensorReading sensorReading) throws Exception {
//                return new Tuple2<>(sensorReading.getId(), "normal");
//            }
//        });

        resultMap.print();

        env.execute();
    }

}