package org.HFC.SQM.task;

import org.HFC.SQM.entity.PackagingMachineSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class PackagingMachineJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 添加自定义数据源
        DataStream<PackagingMachineSource.MachineData> machineStream =
                env.addSource(new PackagingMachineSource())
                        .setParallelism(1); // 确保单并行度维护状态

        // 将DataStream转换为Table
        Table machineTable = tableEnv.fromDataStream(
                machineStream,
                $("id"),
                $("time").rowtime().as("event_time"),
                $("output"),
                $("status"),
                $("uptime")
        );

        // 注册视图
        tableEnv.createTemporaryView("machines", machineTable);

        // 示例SQL查询：实时统计每台机器的总产量
        Table result = tableEnv.sqlQuery(
                "SELECT id, SUM(output) AS total_output " +
                        "FROM machines " +
                        "GROUP BY id"
        );

        // 输出结果
        tableEnv.toChangelogStream(result).print();

        env.execute("Packaging Machine Simulation");
    }
}