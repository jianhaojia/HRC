package org.HFC.SQM.task;

import org.HFC.SQM.utils.ConfigLoader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;

public abstract class BaseTask {
    private static Logger logger = LoggerFactory.getLogger(BaseTask.class);
    //创建 parametertool 获取配置文件
    public static ParameterTool parameterTool = null;
    // 设置静态代码块启动程序的时候初始化所有的参数
    static {
        try {
            parameterTool = ParameterTool
                    .fromPropertiesFile(BaseTask.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            logger.error("BaseTask当前加载配置文件conf.properties异常:"+e.getMessage());
        }
    }
    public static StreamExecutionEnvironment getEnv(String jobName){
        //设置模拟用户
        System.setProperty("HADOOP_USER_NAME","root");
        //todo 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //在设置 env ，将当前的获取到的配置参数传递到 runtime ，在当前的任务中都能获取到参数
        //设置当前的时间使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(parameterTool);
        //todo 2.设置并行度 ①配置文件并行度设置 ②客户端设置 flink run -p 2 ③在程序中 env.setParallel(2) ④算子上并行度（级别最高）
        env.setParallelism(1);
        //todo 3.开启checkpoint及相应的配置，最大容忍次数，最大并行checkpoint个数，checkpoint间最短间隔时间，checkpoint的最大
        env.enableCheckpointing(10 * 1000);
        try {
            env.setStateBackend(new RocksDBStateBackend(parameterTool.getRequired("hdfsUri") + "/flink-checkpoints/"+jobName));
        } catch (IOException e) {
            logger.error("设置statebackend异常");
        }
        //todo 容忍的超时时间，checkpoint如果取消是否删除checkpoint 等
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup
                .RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //todo 4.开启重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(13, Time.seconds(5)));
        return env;
    }
    public static <T> DataStream<T> createKafkaStream(StreamExecutionEnvironment env, Class<? extends DeserializationSchema> clazz) {
        //todo 5. 读取kafka中的数据
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,parameterTool.getRequired("bootstrap.servers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConfigLoader.getProperty("kafka.group.id"));
        //props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //自动探测发现 flink 分区
        props.setProperty(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,"30000");
        //todo 5.1 设置 FlinkKafkaConsumer
        FlinkKafkaConsumer<T> consumer = null;
        try {
            consumer = new FlinkKafkaConsumer<T>(
                    parameterTool.getRequired("kafka.topic"),
                    clazz.newInstance(),
                    props
            );
        } catch (InstantiationException e) {
            logger.error("BaseTask:createKafkaStream:"+e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("BaseTask:createKafkaStream:"+e.getMessage());
        }
        //todo 5.2 配置参数
        // 将 offset 的最新记录存储到 checkpoint ，如果程序失败可以从 checkpoint 中读取最新的数据
        //todo 5.3 消费 kafka 的offset 提交给 flink 来管理
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromEarliest();
        //todo 6 env.addSource
        DataStreamSource<T> source = env.addSource(consumer);
        return source;
    }
    static SinkFunction<String> sinkToHDFS(String outputPath) {
        String hdfsUri = parameterTool.getRequired("hdfsUri");
        return new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                // 创建HDFS文件系统对象
                    FileSystem fs = FileSystem.get(URI.create(hdfsUri));

                // 创建输出流
                Path path = new Path(outputPath);
                FSDataOutputStream outputStream = fs.create(path, true);

                // 写入数据
                outputStream.write(value.getBytes(StandardCharsets.UTF_8));

                // 关闭输出流
                outputStream.close();
            }
        };
}
}
