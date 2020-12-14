package com.swenchao.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Swenchao
 * @Date: 2020/12/14 15:36
 * @Description: 流处理 wc
 * @Modified: NULL
 * @Version: 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 获得实时处理的环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发
        streamEnv.setParallelism(4);

        // 从文件中取数据
        String inputPath = "src/main/resources/WordCount.txt";
        DataStream<String> stringDataStream = streamEnv.readTextFile(inputPath);

        //

        // 转换计算
        DataStream<Tuple2<String, Integer>> resultStream =
                stringDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);

        resultStream.print();

        streamEnv.execute();
    }
}
