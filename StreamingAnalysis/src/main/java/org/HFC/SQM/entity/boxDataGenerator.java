package org.HFC.SQM.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.ThreadUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class boxDataGenerator {

    private static class MachineState {
        public int id;
        public boolean isDown;
        public int remainingDowntime;
        public int uptime;

        public MachineState() {}

        public MachineState(int id, boolean isDown, int remaining, int uptime) {
            this.id = id;
            this.isDown = isDown;
            this.remainingDowntime = remaining;
            this.uptime = uptime;
        }
    }

    private static class MachineData {
        public int id;
        public String time;
        public float output;
        public String status;
        public int uptime;

        public MachineData() {}

        public MachineData(int id, long time, float output, String status, int uptime) {
            this.id = id;
            this.time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
            this.output = output;
            this.status = status;
            this.uptime = uptime;
        }

        @Override
        public String toString() {
            return String.format(
                    "ID: %d, Time: %s, Output: %d, Status: %s, Uptime: %ds",
                    id, time, output, status, uptime
            );
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        List<MachineState> machineStates = new ArrayList<>();
        for (int id = 6; id <= 7; id++) {
            machineStates.add(new MachineState(id, false, 0, 0));
        }

        ObjectMapper objectMapper = new ObjectMapper();
        // 注册自定义序列化器
        SimpleModule module = new SimpleModule();
        module.addSerializer(Long.class, new TimestampSerializer());
        objectMapper.registerModule(module);

        FileWriter fileWriter = new FileWriter("box_data.txt", true);

        boolean isRunning = true;
        while (isRunning) {
            for (MachineState ms : machineStates) {
                long currentTime = System.currentTimeMillis();
                boolean newIsDown = ms.isDown;
                int newRemaining = ms.remainingDowntime;
                int newUptime = ms.uptime;
                int output = 0;

                if (ms.isDown) {
                    newRemaining = Math.max(0, ms.remainingDowntime - 5);
                    if (newRemaining == 0) newIsDown = false;
                } else {
                    double rand = Math.random();
                    if (rand < 0.2) {
                        newIsDown = true;
                        newRemaining = 60;
                    } else if (rand < 0.25) {
                        newIsDown = true;
                        newRemaining = 120;
                    } else {
                        output = 2 + (int) (Math.random() * 2); // 10-15
                        newUptime += 5;
                    }
                }

                MachineData data = new MachineData(
                        ms.id,
                        currentTime,
                        output,
                        newIsDown ? "DOWN" : "RUNNING",
                        newUptime
                );

                // 将数据转换为 JSON 并写入文件
                String jsonData = objectMapper.writeValueAsString(data);
                System.out.println(jsonData);
                fileWriter.write(jsonData + "\n");
                fileWriter.flush();

                // 更新状态
                ms.isDown = newIsDown;
                ms.remainingDowntime = newRemaining;
                ms.uptime = newUptime;
            }
            ThreadUtils thread = null;
            thread.sleep(Duration.ofSeconds(5)); // 5秒间隔

        }

        fileWriter.close();
    }
}