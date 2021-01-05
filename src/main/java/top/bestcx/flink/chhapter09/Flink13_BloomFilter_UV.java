package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import top.bestcx.flink.bean.UserBehavior;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink13_BloomFilter_UV {

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
                .flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
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
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new UvCntBloomFilter(),
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                out.collect("uv值=" + elements.iterator().next() + ",窗口为[" + start + "," + end + ")");
                            }
                        })
                .print();


        env.execute();
    }

    public static class UvCntBloomFilter implements AggregateFunction<Tuple2<String, Long>, Tuple2<BloomFilter<Long>, Long>, Long>{

        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            BloomFilter<Long> longBloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01D);
            return Tuple2.of(longBloomFilter, 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Tuple2<String, Long> value, Tuple2<BloomFilter<Long>, Long> accumulator) {
            Long userId = value.f1;
            BloomFilter<Long> bloomFilter = accumulator.f0;
            Long count = accumulator.f1;
            // 通过 bloom判断是否存在，如果不存在 => count值 + 1， 把对应的格子置为 1
            if (!bloomFilter.mightContain(userId)) {
                // 不存在
                count++;
                bloomFilter.put(userId);
            }
            return Tuple2.of(bloomFilter, count);
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }
}
