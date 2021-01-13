package com.swenchao.apitest.source;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @author : swenchao
 * create at:  2021/1/12  3:07 下午
 * @description: 从自定义source中读取数据
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义source读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());
        // 打印
        dataStream.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义标识位，控制数据产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 定义随机数发生器
            Random random = new Random();

            // 设置10个传感器初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0 ; i < 10 ; ++i) {
                // 随机高斯分布，范围：0 - 120
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            // 标识位为true生成数据

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newtemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newtemp));
                }

                // 控制输出频率
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}