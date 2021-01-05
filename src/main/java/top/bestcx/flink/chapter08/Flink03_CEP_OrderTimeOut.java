package top.bestcx.flink.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.bestcx.flink.bean.OrderEvent;

import java.util.List;
import java.util.Map;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink03_CEP_OrderTimeOut {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        KeyedStream<OrderEvent, Long> orderKS = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((data, ts) -> data.getEventTime() * 1000))
                .keyBy(OrderEvent::getOrderId);


        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("start")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("follow")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<String> main = CEP.pattern(orderKS, pattern).select(
                outputTag,
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        return map.toString() + "--------" + l;
                    }
                }
                ,
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> map) throws Exception {
                        return map.toString();
                    }
                });
        DataStream<String> output = main.getSideOutput(outputTag);
        main.print("main");
        output.print("output");

        env.execute();
    }
}
