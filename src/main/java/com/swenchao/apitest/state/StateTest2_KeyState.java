package com.swenchao.apitest.state;

import apple.laf.JRSUIState;
import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.print.attribute.IntegerSyntax;

/**
 * @author: Swenchao
 * @description: 键控状态
 * @create: 2021-05-22 20:30
 **/
public class StateTest2_KeyState {
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

        // 定义一个有状态map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = mapStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();
    }

    // 自定义MapFunction（必须是富函数）
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;

        // 其他类型状态声明
        private ListState<String> myListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "key-count", Integer.class));

            // 其他类型状态声明（注意多个state，名称不可重复）
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            if (count == null) {
                count = 0;
            }
            ++count;
            keyCountState.update(count);
            return count;

            // 其他状态API调用（du）
//          Iterable<String> strings = myListState.get();
        }
    }
}
