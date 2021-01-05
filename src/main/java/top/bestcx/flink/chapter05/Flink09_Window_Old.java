package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.bestcx.flink.bean.WaterSensor;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
public class Flink09_Window_Old {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
//        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
//                .<WaterSensor>forMonotonousTimestamps()
//                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//                    @Override
//                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
//                        return element.getTs() * 1000;
//                    }
//                });

        DataStreamSource<String> input = environment.socketTextStream("hadoop102", 9999);
        input
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(
//                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
//                            @Override
//                            public long extractTimestamp(WaterSensor element) {
//                                return element.getTs() * 1000;
//                            }
//                        }
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000;
                            }
                        }
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum("vc")
                .print();

        environment.execute();
    }
}

