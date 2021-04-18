package com.swenchao.apitest.sink;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Author: swenchao
 * Description: sink写入es
 * Date 2021/4/18 3:01 下午
 * Version: 1.0
 */
public class SinkTest3_ElasticSearch {
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

        // 定义链接设置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        mapStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        env.execute();
    }

    // 自定义ES写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入数据（dataSource）
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", sensorReading.getId());
            dataSource.put("temp", String.valueOf(sensorReading.getTemperature()));
            dataSource.put("ts", sensorReading.getTimestamp().toString());

            // 创建请求，发送写入命令
            IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingdata").source(dataSource);

            // 用index发送请求
            requestIndexer.add(indexRequest);
        }
    }
}
