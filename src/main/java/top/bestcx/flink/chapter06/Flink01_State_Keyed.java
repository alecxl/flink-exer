package top.bestcx.flink.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
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
public class Flink01_State_Keyed {

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

        private ValueState<String> valueState;
        private ListState<String> listState;
        private MapState<String,String> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value1", Types.STRING));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("value2",Types.STRING));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("value3", Types.STRING, Types.STRING));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
            System.out.println(ctx.getCurrentKey());
            out.collect("bbb");
            System.out.println(valueState.value());
            for (String s : listState.get()) {
                System.out.println(s);
            }
            System.out.println(mapState.contains("aaa"));
        }
    }
}
