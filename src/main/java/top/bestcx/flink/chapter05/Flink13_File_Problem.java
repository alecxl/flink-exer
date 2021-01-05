package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/23 8:52 下午
 * @description:
 */
public class Flink13_File_Problem {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });

        env.readTextFile("input/sensor-data.log")
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, Object, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {
                        TimeWindow window = context.window();
                        out.collect(
                                "=============================================================="
                                        + "\n当前key=" + key
                                        + "\nwatermark=" + context.currentWatermark()
                                        + "\n窗口[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + "\n一共有" + elements.spliterator().estimateSize() + "条数据"
                                        + "\n==========================================================\n\n\n");
                    }
                })
                .print();
        env.execute();
    }
}



