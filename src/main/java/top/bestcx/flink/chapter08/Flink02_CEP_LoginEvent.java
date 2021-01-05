package top.bestcx.flink.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.bestcx.flink.chapter07.LoginEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: 曹旭
 * @date: 2020/12/27 4:05 下午
 * @description:
 */
public class Flink02_CEP_LoginEvent {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<LoginEvent, Long> loginKS = env
                .readTextFile("input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((data, ts) -> data.getEventTime() * 1000)
                )
                .keyBy(LoginEvent::getUserId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .next("next")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .within(Time.seconds(2));

        CEP.pattern(loginKS,pattern).select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                return map.toString();
            }
        })
                .print();

        env.execute();
    }
}
