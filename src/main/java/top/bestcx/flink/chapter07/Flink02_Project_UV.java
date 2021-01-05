package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.UserBehavior;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink02_Project_UV {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] split = element.split(",");
                                return Long.parseLong(split[4]) * 1000;
                            }
                        })
                )
                .flatMap((String value,Collector<Tuple2<String, Long>> out) -> {
                    String[] split = value.split(",");
                        UserBehavior behavior = new UserBehavior(Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                Integer.valueOf(split[2]),
                                split[3],
                                Long.valueOf(split[4])
                        );
                        if ("pv".equals(behavior.getBehavior())) {
                            out.collect(Tuple2.of("uv", behavior.getUserId()));
                        }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t-> t.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
                    Set<Long> userIds = new HashSet<Long>();
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                        for (Tuple2<String, Long> element : elements) {
                            userIds.add(element.f1);
                        }
                        out.collect((long)userIds.size());
                    }

                })
                .print();



        env.execute();
    }
}
