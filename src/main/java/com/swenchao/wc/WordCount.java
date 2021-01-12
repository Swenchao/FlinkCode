package com.swenchao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: Swenchao
 * @Date: 2020/12/8 19:36
 * @Description: 批处理 WordCount
 * @Modified: NULL
 * @Version: 1.0
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/WordCount.txt";
        DataSet<String> stringDataSource = env.readTextFile(inputPath);

        // 对数据集集进行处理，按空格分词，转换成(word,1)
        System.out.println(stringDataSource);
        // 按照第一个位置进行分组，第二个位置求和
        DataSet<Tuple2<String, Integer>> res =
                stringDataSource.flatMap(new MyFlatMapper()).groupBy(0).sum(1).setParallelism(2);
        res.print();
    }

    // 自定义类，实现 FlatMapper接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//            System.out.println(value);
            // 按空格将单词分开
            String[] value_split = value.split(" ");
            for (String word : value_split) {
                out.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}
