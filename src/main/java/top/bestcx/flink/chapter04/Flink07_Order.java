package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.AdsClickLog;
import top.bestcx.flink.bean.OrderEvent;
import top.bestcx.flink.bean.TxEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink07_Order {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        // 1. 读取Order流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                });
        // 2. 读取交易流
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                });

        orderEventDS
                .connect(txDS)
                .keyBy("txId","txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    private Map<String,OrderEvent> orderMap = new HashMap<>();
                    private Map<String,TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if ( txMap.containsKey(value.getTxId()) ){
                            out.collect(value.getTxId() + "success");
                            txMap.remove(value.getTxId());
                        }else {
                            orderMap.put(value.getTxId(),value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if ( orderMap.containsKey(value.getTxId()) ){
                            out.collect(value.getTxId() + "success");
                            orderMap.remove(value.getTxId());
                        }else {
                            txMap.put(value.getTxId(),value);
                        }
                    }
                })
                .print();


        env.execute();
    }
}
