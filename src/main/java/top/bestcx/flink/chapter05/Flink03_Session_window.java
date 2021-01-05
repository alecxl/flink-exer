package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
 * 数据最后一次来时过去3s窗口自动关闭
 *
 * ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
 *                     @Override
 *                     public long extract(Tuple2<String, Long> element) {
 *                         return element.f0.length() * 1000;
 *                     }
 *                 })
 * 自己动态定义窗口关闭的时间
 */
public class Flink03_Session_window {


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
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
//                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
//                    @Override
//                    public long extract(Tuple2<String, Long> element) {
//                        return element.f0.length() * 1000;
//                    }
//                }))
//
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
                        return accumulator + "";
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge");
                        return a + b;
                    }
                })
                .print();

        env.execute();
    }
}


