package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/22 7:38 下午
 * @description:
 */
public class Flink06_Window_Function {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.socketTextStream("hadoop102",9999)
                .flatMap(( String value, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = value.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word,1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(4)))
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0,value1.f1 + value1.f1);
//                    }
//                })
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, String>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        return value.f1 + accumulator;
                    }

                    @Override
                    public String getResult(Long accumulator) {
                        return accumulator + " res ";
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                })
                .print();

        env.execute();
    }
}
