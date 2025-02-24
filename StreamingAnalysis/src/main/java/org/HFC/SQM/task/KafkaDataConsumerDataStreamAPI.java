package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

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
        // 解析 JSON 数据并转换为 Tuple5
//        DataStream<Tuple5<Integer, String, Integer, String, Integer>> parsedStream = kafkaStream
//                .map(json -> {
//                    // 这里假设 JSON 数据格式为 {"id":1,"time":"2023-10-01","output":100,"status":"active","uptime":3600}
//                    // 使用 JSON 解析库（如 Jackson, Gson）来解析 JSON 字符串
//                    // 这里使用简单的字符串操作来模拟解析
//                    String[] parts = json.replace("{", "").replace("}", "").split(",");
//                    int id = Integer.parseInt(parts[0].split(":")[1].trim());
//                    String time = parts[1].split(":")[1].trim().replace("\"", "");
//                    int output = Integer.parseInt(parts[2].split(":")[1].trim());
//                    String status = parts[3].split(":")[1].trim().replace("\"", "");
//                    int uptime = Integer.parseInt(parts[4].split(":")[1].trim());
//                    return new Tuple5<>(id, time, output, status, uptime);
//                })
//                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.INT, Types.STRING, Types.INT));
//
//        // 打印解析后的数据
//        parsedStream.print();

        // 执行作业
        env.execute("Kafka Data Consumer with DataStream API");
    }
}