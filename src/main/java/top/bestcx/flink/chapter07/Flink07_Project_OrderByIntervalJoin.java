package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.OrderEvent;
import top.bestcx.flink.bean.TxEvent;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink07_Project_OrderByIntervalJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        // 1. 读取Order流
        KeyedStream<OrderEvent, String> orderKeyed = env
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
                .keyBy(OrderEvent::getTxId);
        // 2. 读取交易流
        KeyedStream<TxEvent, String> txKeyed = env
                .readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<TxEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((data, ts) -> data.getEventTime() * 1000))
                .keyBy(TxEvent::getTxId);

    orderKeyed
            .intervalJoin(txKeyed)
            .between(Time.hours(-1),Time.hours(1))
            .process(new ProcessJoinFunction<OrderEvent, TxEvent, String>() {
                @Override
                public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<String> out) throws Exception {
                    out.collect("订单"+left.getOrderId()+"对账成功！");
                }
            })
            .print();

        env.execute();
    }
}
