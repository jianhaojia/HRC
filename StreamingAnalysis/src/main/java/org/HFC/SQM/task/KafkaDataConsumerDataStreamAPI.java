package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.Properties;

public class KafkaDataConsumerDataStreamAPI {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载配置
        ConfigLoader configLoader = new ConfigLoader();
        String topic = configLoader.getProperty("kafka.topic");

        // 设置 Kafka 消费者属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "earliest");
        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        // 添加 Kafka 消费者作为数据源
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        kafkaStream.print();
        // 执行作业
        env.execute("Kafka Data Consumer with DataStream API");
    }
}