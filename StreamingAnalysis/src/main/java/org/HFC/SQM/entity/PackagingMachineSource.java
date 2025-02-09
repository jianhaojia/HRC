package org.HFC.SQM.entity;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class PackagingMachineSource extends RichParallelSourceFunction<PackagingMachineSource.MachineData> {

    private transient ListState<MachineState> state;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<MachineState> descriptor =
                new ListStateDescriptor<>("machineStates", MachineState.class);
        state = getRuntimeContext().getListState(descriptor);

        // 现在可以安全地使用键控状态

        // 初始化状态（仅在第一次运行时）
        if (getRuntimeContext().getIndexOfThisSubtask() == 0 && !state.get().iterator().hasNext()) {
            List<MachineState> initialStates = new ArrayList<>();
            for (int id = 1; id <= 5; id++) {
                initialStates.add(new MachineState(id, false, 0, 0));
            }
            state.update(initialStates);
        }
    }

    @Override
    public void run(SourceContext<MachineData> ctx) throws Exception {
        while (isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                List<MachineState> machineStates = new ArrayList<>();
                state.get().forEach(machineStates::add);

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
                            output = 10 + (int) (Math.random() * 6); // 10-15
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
                    ctx.collect(data);

                    // 更新状态
                    ms.isDown = newIsDown;
                    ms.remainingDowntime = newRemaining;
                    ms.uptime = newUptime;
                }
                state.update(machineStates);
            }
            Thread.sleep(5000); // 5秒间隔
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // 状态类
    public static class MachineState {
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

    // 数据类
    public static class MachineData {
        public int id;
        public long time;
        public int output;
        public String status;
        public int uptime;

        public MachineData() {}

        public MachineData(int id, long time, int output, String status, int uptime) {
            this.id = id;
            this.time = time;
            this.output = output;
            this.status = status;
            this.uptime = uptime;
        }

        public Timestamp getEventTime() {
            return new Timestamp(time);
        }

        @Override
        public String toString() {
            return String.format(
                    "ID: %d, Time: %s, Output: %d, Status: %s, Uptime: %ds",
                    id, new Timestamp(time), output, status, uptime
            );
        }
    }
}