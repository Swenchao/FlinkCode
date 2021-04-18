package com.swenchao.apitest.sink;

import com.swenchao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Author: swenchao
 * Description: Sink写入jdbc
 * Date 2021/4/18 3:02 下午
 * Version: 1.0
 */
public class SinkTest4_Jdbc {
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

        mapStream.addSink(new MyJdbcSink());

        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        //声明连接和预编译语句
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建数据库连接
            conn = DriverManager.getConnection("jdbc://mysql://localhost:3306/test", "user", "123456");
            insertStmt = conn.prepareStatement("insert into sensor_tmp (id, temp) values (?, ?)");
            updateStmt = conn.prepareStatement("update sensor_tmp set temp = ? where id = ?");

        }

        @Override
        // 来数据，执行sql
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接执行更新语句，若失败，则插入
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(1, value.getId());
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
