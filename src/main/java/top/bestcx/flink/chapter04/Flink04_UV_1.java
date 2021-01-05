package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.UserBehavior;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink04_UV_1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source
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
                .process(new ProcessFunction<Tuple2<String, Long>, Long>() {
                    Set<Long> userIds = new HashSet<Long>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect((long)userIds.size());
                    }
                })
                .print();



        env.execute();
    }
}
