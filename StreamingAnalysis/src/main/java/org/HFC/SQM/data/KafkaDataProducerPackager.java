package org.HFC.SQM.data;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.kafka.clients.producer.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaDataProducerPackager {
    public static void main(String[] args) {
        ConfigLoader configLoader = new ConfigLoader();
        String topic = configLoader.getProperty("kafka.topic");
        // 配置 Kafka 生产者属性
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka 服务器地址
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建 Kafka 生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 定义 Kafka 主题

        // 读取文件路径
        String filePath = "F:/AI/backup/HRC/machine_data.txt";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 创建 Kafka 消息
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                // 发送消息到 Kafka
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Failed to send message: " + exception.getMessage());
                        } else {
                            System.out.printf("Sent message to partition %d, offset %d%n", metadata.partition(), metadata.offset());
                        }
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        } finally {
            // 关闭生产者
            producer.close();
        }
    }
}
