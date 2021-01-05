package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.UserBehavior;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink03_PV_3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
//                        String[] split = value.split(",");
//                        UserBehavior behavior = new UserBehavior(Long.valueOf(split[0]),
//                                Long.valueOf(split[1]),
//                                Integer.valueOf(split[2]),
//                                split[3],
//                                Long.valueOf(split[4])
//                        );
//                        if ("pv".equals(behavior.getBehavior())) {
//                            out.collect(Tuple2.of("pv", 1L));
//                        }
//                    }
//                })
                .flatMap((String value,Collector<Tuple2<String, Long>> out) -> {
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
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0,value1.f1 + value2.f1);
//                    }
//                })
                .reduce(((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1)))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .print();


        env.execute();
    }
}
