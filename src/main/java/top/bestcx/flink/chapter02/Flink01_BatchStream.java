package top.bestcx.flink.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/18 12:24 下午
 * @description:
 */
public class Flink01_BatchStream {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = environment.readTextFile("input");
        source
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
//                        String[] split = s.split(" ");
//                        for (String s1 : split) {
//                            collector.collect(Tuple2.of(s1, 1L));
//                        }
//                    }
//                })
                .flatMap((String s,Collector<Tuple2<String, Long>> collector) -> {
                    String[] split = s.split(" ");
                        for (String s1 : split) {
                            collector.collect(Tuple2.of(s1, 1L));
                        }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
