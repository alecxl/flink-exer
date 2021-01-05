package top.bestcx.flink.chapter06;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/25 7:21 下午
 * @description:
 */
public class Flink03_State_Broadcast {

    static MapStateDescriptor<String, String> broadcastStateDescriptor = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.STRING);
    static String broadcastKey = "switch";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlDS = env.socketTextStream("hadoop102", 8888);

        BroadcastStream<String> controlBS = controlDS.broadcast(broadcastStateDescriptor);

        BroadcastConnectedStream<String, String> connect = inputDS.connect(controlBS);

        connect.process(new MyBroadcastProcessFunction()).print();


        env.execute();
    }

    public static class MyBroadcastProcessFunction extends BroadcastProcessFunction<String,String,String>{

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
            if ("1".equals(broadcastState.get(broadcastKey))){
                out.collect("1");
            }else {
                out.collect("other");
            }
        }

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(broadcastStateDescriptor).put(broadcastKey,value);
        }
    }
}
