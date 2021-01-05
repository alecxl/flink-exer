package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.WaterSensor;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/23 8:52 下午
 * @description:
 */
public class Flink21_ProcessFunction_Timer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });

        env.socketTextStream("hadoop102",9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(WaterSensor::getId)
                .process(new MyFunction())
                .print();
        env.execute();
    }
    
    public static class MyFunction extends KeyedProcessFunction<String,WaterSensor,String> {
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//            long registerTs = 5000L;
            long registerTs = 5000L + ctx.timestamp();
            ctx.timerService().registerEventTimeTimer(registerTs);
            System.out.println("注册了一个" + new Timestamp(registerTs) + "的定时器");
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器触发了，timestamp=" + new Timestamp(timestamp)
                    + "\nkey=" + ctx.getCurrentKey());
        }
    }
}