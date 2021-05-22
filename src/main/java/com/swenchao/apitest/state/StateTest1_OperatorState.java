package com.swenchao.apitest.state;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author: Swenchao
 * @description: 算子中状态使用
 * @create: 2021-05-19 20:56
 **/
public class StateTest1_OperatorState {
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
        SingleOutputStreamOperator<Integer> resultStream = mapStream.map(new MyCountMapper());

        resultStream.print();

        env.execute();
    }

    // 自定义MapFunction
    // 实现检查点类，保证出错恢复（算子中的列表状态）
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        // 定义本地变量作为算子状态

        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            ++count;
            return count;
        }

        @Override
        // 对状态做快照片
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        // 发生故障之后恢复现场
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
