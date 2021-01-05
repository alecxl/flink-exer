package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.OrderEvent;
import top.bestcx.flink.bean.TxEvent;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink08_Project_OrderTimeOut {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        env
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
                .keyBy(OrderEvent::getOrderId)
                .process(new KeyedProcessFunction<Long, OrderEvent, String>() {

                    ValueState<OrderEvent> payState;
                    ValueState<OrderEvent> createState;
                    ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", Types.POJO(OrderEvent.class)));
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", Types.POJO(OrderEvent.class)));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Types.LONG));
                    }

                    @Override
                    public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if ("create".equals(value.getEventType())) {
                            if (payState.value() == null) {
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15 * 60 * 1000);
                                timerTs.update(ctx.timestamp() + 15 * 60 * 1000);
                                createState.update(value);
                            } else {
                                if (payState.value().getEventTime() - value.getEventTime() > 15 * 60) {
                                    out.collect("订单超时支付,业务系统出现bug");
                                } else {
                                    out.collect(value.getOrderId() + " 对账成功");
                                }
                                ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                timerTs.clear();
                                payState.clear();
                            }
                        } else if ("pay".equals(value.getEventType())) {
                            if (createState.value() == null) {
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                                timerTs.update(ctx.timestamp());
                                payState.update(value);
                            } else {
                                if (value.getEventTime() - createState.value().getEventTime() > 15 * 60) {
                                    out.collect("订单超时支付,业务系统出现bug");
                                } else {
                                    out.collect(value.getOrderId() + " 对账成功");
                                }
                                ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                timerTs.clear();
                                createState.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        if ( payState.value() != null){
                            out.collect("未找到对应创建的订单");
                        }else {
                            out.collect("订单超时");
                        }
                        createState.clear();
                        payState.clear();
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
