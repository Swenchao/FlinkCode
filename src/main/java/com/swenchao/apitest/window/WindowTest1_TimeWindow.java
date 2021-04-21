package com.swenchao.apitest.window;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Author: swenchao
 * Description: 时间窗口使用
 * Date 2021/4/18 9:02 下午
 * Version: 1.0
 */
public class WindowTest1_TimeWindow {
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

        // 增量聚合函数
        SingleOutputStreamOperator<Integer> resultStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                // 第一个Integer为累加器类型，第二个为输出
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    // 创建累加器
                    public Integer createAccumulator() {
                        // 初始值为 0
                        return 0;
                    }

                    @Override
                    // sensorReading：传过来的传感器数据   accumulator：累加器
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        // 来一条数据就加1
                        return accumulator + 1;
                    }

                    @Override
                    // 返回结果
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    // 一般用于会话窗口中（合并分区）
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

//        resultStream.print();

        // 全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                /*
                 * @param <IN> The type of the input value.
                 * @param <OUT> The type of the output value.
                 * @param <KEY> The type of the key.
                 * @param <W> The type of {@code Window} that this window function can be applied on.
                 */
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long timeEnd  = window.getEnd();
                        int count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, timeEnd, count));
                    }
                });

        resultStream2.print();

        env.execute();
    }
}
