package org.HFC.SQM.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

public class WIPCalculationJob {
    private static final ConfigLoader configLoader = new ConfigLoader();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "wip-calculation");

        // 原始数据流
        DataStream<DeviceEvent> rawPackageStream = env
                .addSource(new FlinkKafkaConsumer<>("package-merge", new JSONDeserializationSchema(), properties));

        DataStream<DeviceEvent> rawBoxStream = env
                .addSource(new FlinkKafkaConsumer<>("box-merge", new JSONDeserializationSchema(), properties));

        // 动态分组处理
        DataStream<GroupedEvent> packageStream = rawPackageStream
                .map(new GroupMapper("P"))
                .filter(event -> event != null);

        DataStream<GroupedEvent> boxStream = rawBoxStream
                .map(new GroupMapper("X"))
                .filter(event -> event != null);

        // 分配时间戳和水印
        WatermarkStrategy<GroupedEvent> watermarkStrategy = WatermarkStrategy
                .<GroupedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> parseTimeToMillis(event.getTime()));

        DataStream<GroupedEvent> processedPackageStream = packageStream.assignTimestampsAndWatermarks(watermarkStrategy);
        DataStream<GroupedEvent> processedBoxStream = boxStream.assignTimestampsAndWatermarks(watermarkStrategy);

        // 计算各组汇总值
        DataStream<GroupSum> pSumStream = processedPackageStream
                .keyBy(GroupedEvent::getGroupName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SumAggregator());

        DataStream<GroupSum> bSumStream = processedBoxStream
                .keyBy(GroupedEvent::getGroupName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SumAggregator());

        // 关联计算中间制品数
        pSumStream.connect(bSumStream)
                .keyBy(p -> extractBaseGroup(p.groupName),
                        b -> extractBaseGroup(b.groupName))
                .process(new WIPCalculator())
                .print();

        env.execute("Dynamic WIP Calculation");
    }

    // 动态分组映射逻辑
    private static class GroupMapper implements MapFunction<DeviceEvent, GroupedEvent> {
        private final String typeSuffix;

        GroupMapper(String typeSuffix) {
            this.typeSuffix = typeSuffix;
        }

        @Override
        public GroupedEvent map(DeviceEvent event) {
            String groupName = resolveGroupName(event.getId(), typeSuffix);
            return groupName != null ? new GroupedEvent(groupName, event) : null;
        }

        private String resolveGroupName(int id, String suffix) {
            if ("P".equals(suffix)) {
                if (id >= 1 && id <= 5) return "883_A01_P";
                if (id >= 8 && id <= 12) return "883_A02_P";
            } else if ("X".equals(suffix)) {
                if (id >= 6 && id <= 7) return "883_A01_X";
            }
            return null;
        }
    }

    // 在制品计算逻辑
    private static class WIPCalculator extends CoProcessFunction<GroupSum, GroupSum, String> {
        private ValueState<Integer> pSumState;
        private ValueState<Integer> bSumState;

        @Override
        public void open(Configuration parameters) {
            pSumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pSumState", Integer.class));
            bSumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("bSumState", Integer.class));
        }

        @Override
        public void processElement1(GroupSum pSum, Context ctx, Collector<String> out) throws Exception {
            handleSum(pSum, true, out);
        }

        @Override
        public void processElement2(GroupSum bSum, Context ctx, Collector<String> out) throws Exception {
            handleSum(bSum, false, out);
        }

        private void handleSum(GroupSum sum, boolean isProduction, Collector<String> out) throws Exception {
            String baseGroup = extractBaseGroup(sum.groupName);

            if (isProduction) {
                pSumState.update(sum.sum);
            } else {
                bSumState.update(sum.sum);
            }

            Integer pVal = pSumState.value();
            Integer bVal = bSumState.value();

            if (pVal != null && bVal != null) {
                int wip = pVal - 5 * bVal;
                out.collect(String.format("%s_P与%s_X 在制品数: %d", baseGroup, baseGroup, wip));
                pSumState.clear();
                bSumState.clear();
            }
        }
    }

    // 数据结构定义
    public static class DeviceEvent {
        private int id;
        private int totalOutput;
        private String time;

        // Getters and Setters
        public int getId() { return id; }
        public int getTotalOutput() { return totalOutput; }
        public String getTime() { return time; }
        public void setId(int id) { this.id = id; }
        public void setTotalOutput(int totalOutput) { this.totalOutput = totalOutput; }
        public void setTime(String time) { this.time = time; }
    }

    public static class GroupedEvent {
        private final String groupName;
        private final DeviceEvent event;

        public GroupedEvent(String groupName, DeviceEvent event) {
            this.groupName = groupName;
            this.event = event;
        }

        public String getGroupName() { return groupName; }
        public int getTotalOutput() { return event.getTotalOutput(); }
        public String getTime() { return event.getTime(); }
    }

    public static class GroupSum {
        public String groupName;
        public int sum;

        public GroupSum() {}

        public GroupSum(String groupName, int sum) {
            this.groupName = groupName;
            this.sum = sum;
        }
    }

    // 聚合函数实现
    public static class SumAggregator implements AggregateFunction<GroupedEvent, SumAggregator.Accumulator, GroupSum> {
        public static class Accumulator {
            String groupName;
            int sum;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(GroupedEvent value, Accumulator accumulator) {
            if (accumulator.groupName == null) {
                accumulator.groupName = value.getGroupName();
            }
            accumulator.sum += value.getTotalOutput();
            return accumulator;
        }

        @Override
        public GroupSum getResult(Accumulator accumulator) {
            return new GroupSum(accumulator.groupName, accumulator.sum);
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            a.sum += b.sum;
            return a;
        }
    }

    // 工具方法
    private static String extractBaseGroup(String groupName) {
        return groupName.substring(0, groupName.lastIndexOf('_'));
    }

    private static long parseTimeToMillis(String time) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
        } catch (Exception e) {
            throw new RuntimeException("时间解析失败: " + time, e);
        }
    }

    // JSON反序列化实现
    public static class JSONDeserializationSchema implements DeserializationSchema<DeviceEvent> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public DeviceEvent deserialize(byte[] message) {
            try {
                return mapper.readValue(message, DeviceEvent.class);
            } catch (IOException e) {
                throw new RuntimeException("JSON解析失败", e);
            }
        }

        @Override
        public boolean isEndOfStream(DeviceEvent nextElement) {
            return false;
        }

        @Override
        public TypeInformation<DeviceEvent> getProducedType() {
            return TypeInformation.of(DeviceEvent.class);
        }
    }
}