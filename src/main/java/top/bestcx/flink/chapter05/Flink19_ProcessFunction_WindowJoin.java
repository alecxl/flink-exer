package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink19_ProcessFunction_WindowJoin {

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

        s1
                .join(s2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new MyFunction())
                .print();
        env.execute();
    }

    public static class MyFunction implements JoinFunction<Tuple2<String, Integer>,Tuple3<String, Integer, Integer>,String>{

        @Override
        public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
            return "first" + first.f0 + " second" + second.f0;
        }
    }

}
