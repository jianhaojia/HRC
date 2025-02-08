package org.HFC.SQM.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.HFC.SQM.entity.Packager;
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.HFC.SQM.task.BaseTask.*;

public class KafkaSourceDataTask extends BaseTask {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = getEnv(KafkaSourceDataTask.class.getSimpleName());


        // 从Kafka读取数据
        DataStream<String> source = createKafkaStream(env, SimpleStringSchema.class);
        //source把string类型转换为Packager类型


        // 将JSON字符串解析为Packager对象
        DataStream<Packager> packagerStream = source.map(new MapFunction<String, Packager>() {
            @Override
            public Packager map(String value) throws Exception {
                JSONObject jsonObject = new JSONObject(value);
                Packager packager = new Packager();
                packager.setId(jsonObject.getInt("id"));
                packager.setTime(LocalDateTime.parse(jsonObject.getString("time")));
                packager.setProduction(jsonObject.getInt("production"));
                packager.setStatus(jsonObject.getString("status"));
                packager.setUptime(jsonObject.getDouble("uptime"));
                return packager;
            }
        });

        packagerStream.print();

    // 将Packager对象写入HDFS
        packagerStream.map(new MapFunction<Packager, String>() {
            @Override
            public String map(Packager value) throws Exception {
                return value.getId() + "," + value.getTime() + "," + value.getProduction() + "," + value.getStatus() + "," + value.getUptime();
            }
        }).addSink(sinkToHDFS(parameterTool.get("hdfsUri")+"/apps/hive/warehouse/ods.db/packager"));


        // 执行任务
        env.execute("Kafka Source Data Task");
    }


// 将DataStream写入HDFS的方法


}
