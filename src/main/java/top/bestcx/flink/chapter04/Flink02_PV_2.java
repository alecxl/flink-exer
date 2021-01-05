package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.UserBehavior;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink02_PV_2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] split = value.split(",");
                        UserBehavior behavior = new UserBehavior(Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                Integer.valueOf(split[2]),
                                split[3],
                                Long.valueOf(split[4])
                        );
                        if ("pv".equals(behavior.getBehavior())) {
                            out.collect(Tuple2.of("pv", 1L));
                        }
                    }
                })
                .process(new ProcessFunction<Tuple2<String, Long>, Long>() {
                    Long v = 0L;
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                        v++;
                        out.collect(v);
                    }
                })
                .print();

        env.execute();
    }
}