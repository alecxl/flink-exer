package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink20_ProcessFunction_IntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> s1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 2),
                        Tuple2.of("c", 3),
                        Tuple2.of("d", 4)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((data, ts) -> data.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> s2 = env.fromElements(
                Tuple3.of("a", 1, 111),
                Tuple3.of("a", 4, 211),
                Tuple3.of("b", 3, 311),
                Tuple3.of("c", 4, 411)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((data, ts) -> data.f1 * 1000L)
                );
        KeyedStream<Tuple2<String, Integer>, String> k1 = s1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> k2 = s2.keyBy(r -> r.f0);

        k1
              .intervalJoin(k2)
                .between(Time.seconds(-1),Time.seconds(2))
                .process(new MyFunction())
                .print();
        env.execute();
    }

    public static class MyFunction extends ProcessJoinFunction<Tuple2<String, Integer>,Tuple3<String, Integer, Integer>,String>{

        @Override
        public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
            System.out.println(ctx.getTimestamp());
            System.out.println(ctx.getLeftTimestamp());
            System.out.println(ctx.getRightTimestamp());
            out.collect(left.toString() +  " " + right.toString()) ;
        }
    }

}
