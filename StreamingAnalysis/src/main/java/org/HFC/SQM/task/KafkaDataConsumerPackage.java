package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.HFC.SQM.utils.QQEmailSender;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaDataConsumerPackage {
    private static ConfigLoader configLoader = new ConfigLoader();
    private static final List<Integer> ALARM_THRESHOLDS = Arrays.asList(60, 300, 600);
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载配置

        String topic = configLoader.getProperty("kafka.packager.topic");
        String url = configLoader.getProperty("clikhouse.url");
        String user = configLoader.getProperty("clikhouse.user");
        String password = configLoader.getProperty("clikhouse.password");
        String testFlag="True";
        // 设置 Kafka 消费者属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "latest");

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        // 添加 Kafka 消费者作为数据源
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        env.setParallelism(1);
        // ... existing code ...
        kafkaStream = kafkaStream.keyBy(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                Integer key = jsonObject.getInt("id");
                return key;
            }
        }).process(new KeyedProcessFunction<Object, String, String>() {
            // 定义一个 ValueState 用于存储每个 key 的 output 总和
            private transient ValueState<Double> totalOutputState;
            // 定义一个 ValueState 用于存储每个 key 的停机时间
            private transient ValueState<Double> downTimeState;
            // 定义一个 ValueState 用于存储每个 key 的上次处理时间
            private transient ValueState<Long> lastProcessTimeState;

            private transient MapState<Integer, Boolean> alarmTriggeredMapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                ValueStateDescriptor<Double> totalOutputDescriptor = new ValueStateDescriptor<>(
                        "totalOutput", // 状态名称
                        Types.DOUBLE // 状态类型
                );
                totalOutputState = getRuntimeContext().getState(totalOutputDescriptor);

                ValueStateDescriptor<Double> downTimeDescriptor = new ValueStateDescriptor<>(
                        "downTime", // 状态名称
                        Types.DOUBLE // 状态类型
                );
                downTimeState = getRuntimeContext().getState(downTimeDescriptor);

                MapStateDescriptor<Integer, Boolean> alarmMapDescriptor = new MapStateDescriptor<>(
                        "alarmTriggers", Types.INT, Types.BOOLEAN
                );
                alarmTriggeredMapState = getRuntimeContext().getMapState(alarmMapDescriptor);

                ValueStateDescriptor<Long> lastProcessTimeDescriptor = new ValueStateDescriptor<>(
                        "lastProcessTime", // 状态名称
                        Types.LONG // 状态类型
                );
                lastProcessTimeState = getRuntimeContext().getState(lastProcessTimeDescriptor);
            }

            @Override
            public void processElement(String s, KeyedProcessFunction<Object, String, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                // 获取当前元素的 output 值
                double output = jsonObject.getDouble("output");
                // 获取当前元素的 status 值
                String status = jsonObject.getString("status");
                // 获取当前元素的 time 值
                String timeStr = jsonObject.getString("time");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long currentTime = sdf.parse(timeStr).getTime();

                // 获取当前 key 的 total_output 状态
                Double totalOutput = totalOutputState.value();
                if (totalOutput == null) {
                    totalOutput = 0.0;
                }

                // 累加 output 值
                totalOutput += output;
                // 更新状态
                totalOutputState.update(totalOutput);

                // 获取当前 key 的停机时间状态
                Double downTime = downTimeState.value();
                if (downTime == null) {
                    downTime = 0.0;
                }

                // 获取当前 key 的上次处理时间状态
                Long lastProcessTime = lastProcessTimeState.value();

                if (!"RUNNING".equals(status) && lastProcessTime != null) {
                    // 如果状态不是 RUNNING，计算停机时间并累加
                    downTime += (currentTime - lastProcessTime) / 1000.0;
                }

                // 更新上次处理时间
                lastProcessTimeState.update(currentTime);
                // 更新停机时间状态
                downTimeState.update(downTime);

                // 添加 total_output 字段到 JSON 对象
                jsonObject.put("total_output", totalOutput);
                // 添加 down_time 字段到 JSON 对象
                jsonObject.put("down_time", downTime);
                for (Integer threshold : ALARM_THRESHOLDS) {
                    Boolean hasTriggered = alarmTriggeredMapState.get(threshold);
                    if (downTime >= threshold && (hasTriggered == null || !hasTriggered)) {
                        // 构建报警信息（可添加阈值参数）
                        JSONObject alarmInfo = new JSONObject();
                        alarmInfo.put("id", jsonObject.getInt("id"));
                        alarmInfo.put("down_time", downTime);
                        alarmInfo.put("threshold", threshold);

                        QQEmailSender.errorNoteEmail(testFlag, alarmInfo.toString());

                        // 标记该阈值已触发
                        alarmTriggeredMapState.put(threshold, true);
                    }
                }

                // 输出处理后的 JSON 字符串
                collector.collect(jsonObject.toString());
            }
        });
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("package-merge")  // 替换为你的输出topic
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProperties)
                .build();
        kafkaStream.sinkTo(kafkaSink);
//        kafkaStream.print();
        DataStream<Row> parsedStream = kafkaStream.map(new MapFunction<String, Row>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public Row map(String value) throws Exception {
                Map<String, Object> data = objectMapper.readValue(value, Map.class);
                Row row = new Row(7);
                row.setField(0, data.get("output"));
                row.setField(1, data.get("total_output"));
                row.setField(2, data.get("down_time"));
                row.setField(3, data.get("id"));
                row.setField(4, Timestamp.valueOf((String) data.get("time")));
                row.setField(5, data.get("status"));
                row.setField(6, data.get("uptime"));
                return row;
            }
        });

        JdbcSink clickHouseSink = (JdbcSink) JdbcSink.sink(
                "INSERT INTO my_table (output, total_output, down_time, id, time, status, uptime) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (statement, row) -> {
                    Row dataRow = (Row) row;
                    statement.setInt(1, (Integer) dataRow.getField(0));
                    statement.setInt(2, (Integer) dataRow.getField(1));
                    statement.setInt(3, (Integer) dataRow.getField(2));
                    statement.setInt(4, (Integer) dataRow.getField(3));
                    statement.setTimestamp(5, (Timestamp) dataRow.getField(4));
                    statement.setString(6, (String) dataRow.getField(5));
                    statement.setInt(7, (Integer) dataRow.getField(6));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        );

// ... existing code ...

        // 执行作业
        env.execute("Kafka Data Consumer with DataStream API");
    }
}