package com.swenchao.apitest.window;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * Author: swenchao
 * Description: 事件时间语义使用（event_time）
 * Date 2021/5/9 9:11 下午
 * Version: 1.0
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TimeCharacteristic是一个枚举类型，枚举了三种时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转化成SensorReading类型watermark
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        })
                // 方式1
                // 升序数据设置时间戳和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                })

                // 方式2
                // 乱序数据设置时间戳和watermark
                // BoundedOutOfOrdernessTimestampExtractor类需要传一个最大乱序程度的值;
                // BoundedOutOfOrdernessTimestampExtractor看底层继承是周期性生成watermark（最大时间戳减掉延迟时间 ）
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        // 时间戳是毫秒数
                        return element.getTimestamp() * 1000L;
                    }
                });

        OutputTag<SensorReading> outputTag = new OutputTag<>("late");

        // 基于事件时间的开窗操作，统计15s内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                // 乱序兜底策略（一分钟允许的延迟）
                .allowedLateness(Time.minutes(1))
                // 侧输出流最终兜底
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        // 获得侧输出刘流
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
