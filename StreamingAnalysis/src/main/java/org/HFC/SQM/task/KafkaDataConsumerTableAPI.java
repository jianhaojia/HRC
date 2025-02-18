package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaDataConsumerTableAPI {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ConfigLoader configLoader = new ConfigLoader();
        String topic = configLoader.getProperty("kafka.topic");

        // 创建 Kafka 数据源表
        String createTableSQL = "CREATE TABLE kafka_table (\n" +
                "    `id` INT,\n" +
                "    `time` STRING,\n" +
                "    `output` INT,\n" +
                "    `status` STRING,\n" +
                "    `uptime` INT\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + topic + "',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";
        tableEnv.executeSql(createTableSQL);

        // 从 Kafka 表中查询数据
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM kafka_table");

        // 将表转换为流并打印
        tableEnv.toDataStream(resultTable, Row.class).print();

        // 执行作业
        env.execute("Kafka Data Consumer with Table API");
    }
}