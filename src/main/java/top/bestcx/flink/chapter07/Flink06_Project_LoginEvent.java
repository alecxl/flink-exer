package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/27 4:05 下午
 * @description:
 */
public class Flink06_Project_LoginEvent {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.valueOf(split[0]),split[1],split[2],Long.valueOf(split[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((data,ts) -> data.getEventTime() * 1000)
                )
                .keyBy(LoginEvent::getUserId)
                .process(new KeyedProcessFunction<Long, LoginEvent, String>() {
                    ListState<LoginEvent> failureEvent;
                    ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception{
                        failureEvent = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failureEvent",Types.POJO(LoginEvent.class)));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs",Types.LONG));
                    }

                    @Override
                    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

                        if ("success".equals(value.getEventType())){
                            if (timerTs.value() != null){
                                ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                timerTs.clear();
                                failureEvent.clear();
                            }
                        }else if ("fail".equals(value.getEventType())){
                            if (timerTs.value() == null){
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 2000);
                                timerTs.update(ctx.timestamp() + 2000);
                            }
                            failureEvent.add(value);
                            if (failureEvent.get().spliterator().estimateSize() >= 2 ){
                                out.collect("用户" + value.getUserId() + "在2s内连续登陆失败2次，可能存在恶意行为！");
                                ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                timerTs.clear();
                                failureEvent.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        if (failureEvent.get().spliterator().estimateSize() >= 2 ){
                            out.collect("用户" + ctx.getCurrentKey() + "在2s内连续登陆失败2次，可能存在恶意行为！");
                        }
                        timerTs.clear();
                        failureEvent.clear();
                    }
                })
                .print();

        env.execute();
    }
}
