package com.swenchao.apitest.window;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author: swenchao
 * Description: TimeWindow使用
 * Date 2021/4/18 9:02 下午
 * Version: 1.0
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> sensorDataStreamSource = env.readTextFile("src/main/resources/sensor.txt");
        // 转换成 SensorReading 类型
        SingleOutputStreamOperator<SensorReading> mapStream = sensorDataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fileds = s.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
            }
        });

        // 窗口测试
        // 基于dataStream可以调windowAll方法
        // Note: This operation is inherently non-parallel since all elements have to pass through
        //	 * the same operator instance.  （源码注解：因为所有元素都被传递到下游相同的算子中，所以本质上是非并行的）
//         mapStream.windowAll();
        // 开窗口前要先进行keyBy
        mapStream.keyBy("id")
                // 时间窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // 根据传参，判断开的是什么窗口（滚动 滑动）
//                .timeWindow(Time.seconds(15))
                .timeWindow(Time.seconds(15), Time.seconds(5));

        env.execute();
    }
}
