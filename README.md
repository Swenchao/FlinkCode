# FlinkCode

Flink学习实践demo

## WordCount

[离线](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/wc/WordCount.java)

[实时](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/wc/StreamWordCount.java)

## 流处理 API

## Source

[从集合读取数据](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/source/SourceTest1_Collection.java)

[从文件中读取数据](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/source/SourceTest2_File.java)

[从Kafka中读取数据](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/source/SourceTest3_Kafka.java)

[从自定义source读取数据](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/source/SourceTest4_UDF.java)

## Transform

### 基本算子：map、flatMap、filter

[基本算子使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest1_Base.java)

### 非基本算子：KeyBy、滚动算子

[滚动算子使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest2_RollingAggregation.java)

### Reduce算子

[Reduce使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest3_Reduce.java)

### 多流转换操作（拆分操作）

[Split、Select使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest4_MultipleStreams.java)

### 多流转换操作（两流合并）

[Connect、CoMap使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest4_MultipleStreams_Connect.java)

### 富函数

[富函数使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest5_RichFunction.java)

### 重分区

[shuffle | keyby使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/transform/TransformTest6_Partitiom.java)

## Sink

### Kafka

[Kafka使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/sink/SinkTest1_Kafka.java)

### Redis

[Redis使用](https://github.com/Swenchao/FlinkCode/blob/main/src/main/java/com/swenchao/apitest/sink/SinkTest2_Redis.java)
