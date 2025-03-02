package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaDataConsumerTableAPI {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建 Table 环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 加载配置
        ConfigLoader configLoader = new ConfigLoader();
        String topic = configLoader.getProperty("kafka.topic");

        // 创建 Kafka 表
        tableEnv.executeSql(
                "CREATE TABLE kafka_json_table (                      \n" +
                        "  id INT,                                            \n" +
                        "  `time` STRING,                                     \n" +
                        "  output INT,                                        \n" +
                        "  status STRING,                                     \n" +
                        "  uptime INT,                                        \n" +
                        "  event_time AS TO_TIMESTAMP(`time`, 'yyyy-MM-dd HH:mm:ss'), \n" +  // 将 time 字段转为时间属性
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND \n" +  // 定义 watermark
                        ") WITH (                                             \n" +
                        "  'connector' = 'kafka',                             \n" +
                        "  'topic' = '" + topic + "',                         \n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092', \n" +
                        "  'properties.group.id' = 'test',                    \n" +
                        "  'scan.startup.mode' = 'earliest-offset',           \n" +
                        "  'format' = 'json'                                  \n" +
                        ")"
        );
//earliest-offset
        // 解析 JSON 数据
        Table parsedTable = tableEnv.sqlQuery(
                "SELECT                                              \n" +
                        "  id,                                               \n" +
                        "  event_time,                                       \n" +
                        "  output,                                           \n" +
                        "  status,                                           \n" +
                        "  uptime                                            \n" +
                        "FROM kafka_json_table                               \n"
        );

        // 开窗计算 output 的 SUM，窗口范围为从最早的行到当前行
//        Table windowedTable = tableEnv.sqlQuery(
//                "SELECT                                              \n" +
//                        "  id,                                               \n" +
//                        "  event_time,                                       \n" +
//                        "  output,                                           \n" +
//                        "  status,                                           \n" +
//                        "  uptime,                                           \n" +
//                        "  SUM(output) OVER (                                \n" +  // 开窗计算 SUM
//                        "    PARTITION BY id                                 \n" +  // 按 id 分区
//                        "    ORDER BY event_time                             \n" +  // 按 event_time 排序
//                        "    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \n" +  // 窗口范围为从最早的行到当前行
//                        "  ) AS window_sum                                  \n" +  // 窗口 SUM 值
//                        "FROM " + parsedTable + "                            \n"  // 使用上一个查询的结果
//        );

        // 打印开窗结果
        parsedTable.execute().print();

        // 显式触发任务执行
        env.execute("Kafka Data Consumer Job");
    }
}