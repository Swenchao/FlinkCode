package com.swenchao.apitest.transform;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : swenchao
 * create at:  2021/1/17  4:21 下午
 * @description: reduce操作
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
//        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fileds = s.split(",");
//                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
//            }
//        });

        // lambda表达式写法
        SingleOutputStreamOperator<SensorReading> mapLambdaStream = dataStream.map(line -> {
            String[] fileds = line.split(",");
            return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
        });

        // 分组（根据id分组）
        // 其中KeyedStream中后一个范型之所以为tuple，是因为其keyby可根据多个标签进行分组
        KeyedStream<SensorReading, Tuple> keyedStream = mapLambdaStream.keyBy("id");
        // 另一种写法（方法引用）
//        KeyedStream<SensorReading, String> keyedStream1 = mapLambdaStream.keyBy(SensorReading::getId);

        // reduce聚合，取最大温度值以及当前最新时间戳
        SingleOutputStreamOperator<SensorReading> resultReduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            // sensorReading当前状态；t1新来数据
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(), t1.getTimestamp(),
                        Math.max(sensorReading.getTemperature(), t1.getTemperature()));
            }
        });

        // lambda表达式方式实现
//        SingleOutputStreamOperator<SensorReading> resultReduce = keyedStream.reduce((curStat, newData) -> {
//            return new SensorReading(curStat.getId(), newData.getTimestamp(),
//                    Math.max(curStat.getTemperature(), newData.getTemperature()));
//        });

        resultReduce.print();
        env.execute();
    }

}