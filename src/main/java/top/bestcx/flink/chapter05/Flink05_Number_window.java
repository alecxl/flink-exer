package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


public class Flink05_Number_window {


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
                .countWindow(3)
                .sum(1)
                .print();

        env.execute();
    }
}


