package org.HFC.SQM.task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DataGenerate {
    private static final Logger logger = LoggerFactory.getLogger(DataGenerate.class);
    // 定义包装机的数量
    private static final int NUM_PACKAGERS = 5;

    // 定义状态列表
    private static final List<String> STATUSES = new ArrayList<>();

    // 定义时间格式
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    static {
        STATUSES.add("running");
    }

    // 定义数据生成函数
private static String generatePackagerData(int packagerId, String currentTime, double uptime) {
    Random random = new Random();
    String status;
    int production;

    // 90%的概率为running，10%的概率为malfunction
    if (random.nextDouble() < 0.9) {
        status = "running";
        production = random.nextInt(6) + 10; // 10-15
    } else {
        status = "malfunction";
        production = random.nextInt(11); // 0-10
    }

    return String.format("{\"id\": %d, \"time\": \"%s\", \"production\": %d, \"status\": \"%s\", \"uptime\": %.2f}",
            packagerId, currentTime, production, status, uptime);
}


    // 自定义数据源函数
    public static class DataSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            List<Double> uptimes = new ArrayList<>();
            Random random = new Random();
            for (int i = 0; i < NUM_PACKAGERS; i++) {
                uptimes.add((double) (random.nextInt(241) + 1200));
            }

            while (isRunning) {
                String currentTime = TIME_FORMAT.format(new Date());
                for (int packagerId = 1; packagerId <= NUM_PACKAGERS; packagerId++) {
                    String data = generatePackagerData(packagerId, currentTime, uptimes.get(packagerId - 1));
                    ctx.collect(data);
                    // 更新uptime值
                    uptimes.set(packagerId - 1, uptimes.get(packagerId - 1) + 0.25);
                }
                Thread.sleep(15000); // 每30秒发送一次数据
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 主函数
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStream<String> dataStream = env.addSource(new DataSource());

        // 打印数据流
        dataStream.print();

        // 执行任务
        env.execute("Flink Data Generation");
    }
}
