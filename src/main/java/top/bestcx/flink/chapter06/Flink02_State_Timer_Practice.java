package top.bestcx.flink.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/23 8:52 下午
 * @description:
 */
public class Flink02_State_Timer_Practice {

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
//        private long timerTS = 0L;
//        private Integer preVC = Integer.MIN_VALUE;
        private ValueState<Integer> preVC;
        private ValueState<Long> timerTS;


        @Override
        public void open(Configuration parameters) throws Exception {
            preVC = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("preVC", Types.INT,0));
            timerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTS", Types.LONG,0L));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 第一次 注册一个定时器
                if (timerTS.value() == 0){
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000 );
                    timerTS.update(ctx.timestamp() + 5000);
                    System.out.println("第一次注册定时器" + ctx.getCurrentKey());
                }

                // 判断水位的变化
                if (value.getVc() < preVC.value()  && ctx.timestamp() <= timerTS.value()){
                    // 如果水位小于 之前的水,重新设置定时器
                    ctx.timerService().deleteEventTimeTimer( timerTS.value());
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000 );
                    timerTS.update( ctx.timestamp() + 5000);
                    System.out.println("水位降低定时器更新" + ctx.getCurrentKey());
                }
                preVC.update(value.getVc());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("报警触发" + ctx.getCurrentKey());
            timerTS.clear();
        }
    }
}
