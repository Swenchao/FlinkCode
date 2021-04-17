package com.swenchao.apitest.sink;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: Swenchao
 * @description: sink到redis
 * @create: 2021-03-31 15:22
 **/
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据进行转换

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

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        DataStreamSink<SensorReading> sensorReadingDataStreamSink = mapStream.addSink(new RedisSink<>(conf,
                new MyRedisMapper()));

        env.execute();
    }


    /**
     * Author: Swenchao
     * Description: 自定义RedisMapper
     * Date 2021/4/16 10:27 上午
     * Version: 1.0
     */
    public static class MyRedisMapper implements RedisMapper<SensorReading> {

        // 定义保存数据到redis的命令，存成Hash表 hset sensor_temp id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            // RedisCommand枚举值，列举redis命令
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return null;
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return null;
        }
    }
}
