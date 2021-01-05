package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.bestcx.flink.bean.WaterSensor;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
class Flink11_Window_Puntanated {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
               .forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                   @Override
                   public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                        return new MyPuntuated();
                   }
               })
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });

        DataStreamSource<String> input = environment.socketTextStream("hadoop102", 9999);
        input
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum("vc")
                .print();

        environment.execute();


    }

    public static class MyPuntuated implements WatermarkGenerator<WaterSensor>{

        private long maxTs = Long.MIN_VALUE;

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs,eventTimestamp);
            output.emitWatermark(new Watermark(maxTs));
            System.out.println("onEvent.....");
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("period....");
        }
    }
}

