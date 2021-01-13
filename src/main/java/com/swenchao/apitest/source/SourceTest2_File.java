package com.swenchao.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : swenchao
 * create at:  2021/1/12  7:34 下午
 * @description: 从文件中读取数据
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");

        dataStream.print();
        env.execute();
    }
}