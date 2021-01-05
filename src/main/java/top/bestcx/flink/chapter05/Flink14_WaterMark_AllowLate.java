package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/23 8:52 下午
 * @description:
 */
public class Flink14_WaterMark_AllowLate {

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
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sum("vc")
                .print();
        env.execute();
    }
}


/*
        env.socketTextStream("hadoop102",9999)
                .flatMap( (String value, Collector<Tuple2<String, Long>> collector) -> {
                    String[] split = value.split(" ");
                    for (String s : split) {
                        collector.collect(Tuple2.of(s,1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t->t.f0 )
                .print();*/
