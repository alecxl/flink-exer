package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/23 8:52 下午
 * @description:
 */
public class Flink21_ProcessFunction_Timer_SideOut {

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

        SingleOutputStreamOperator<String> main = env.socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(WaterSensor::getId)
                .process(new MyFunction());
        DataStream<Long> output = main.getSideOutput(new OutputTag<Long>("warning_time") {
        });
        main.print();
        output.print();


        env.execute();
    }
    
    public static class MyFunction extends KeyedProcessFunction<String,WaterSensor,String> {
        private long timerTS = 0L;
        private Integer preVC = Integer.MIN_VALUE;
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 第一次 注册一个定时器
                if (timerTS == 0L){
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000 );
                    timerTS = ctx.timestamp() + 5000;
                    System.out.println("第一次注册定时器" + ctx.getCurrentKey());
                }

                // 判断水位的变化
                if (value.getVc() < preVC){
                    // 如果水位小于 之前的水,重新设置定时器
                    ctx.timerService().deleteEventTimeTimer( timerTS);
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000 );
                    timerTS = ctx.timestamp() + 5000;
                    System.out.println("水位降低定时器更新" + ctx.getCurrentKey());
                }
                preVC = value.getVc();

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            OutputTag<Long> outputTag = new OutputTag<Long>("warning_time") {
            };
            out.collect("报警触发" + ctx.getCurrentKey());
            timerTS = 0L;
            ctx.output(outputTag,timestamp);
        }
    }
}
