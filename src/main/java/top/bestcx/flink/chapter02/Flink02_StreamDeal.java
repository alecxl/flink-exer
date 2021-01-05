package top.bestcx.flink.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/18 12:44 下午
 * @description:
 */
public class Flink02_StreamDeal {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = environment.readTextFile("input/words.txt");
        input.flatMap((String s, Collector<Tuple2<String, Long>> collector) -> {
            String[] split = s.split(" ");
            for (String s1 : split) {
                collector.collect(Tuple2.of(s1, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
//                        return stringLongTuple2.f0;
//                    }
//                })
                .keyBy(stringLongTuple2 -> {
                    return stringLongTuple2.f0;
                })
                .sum(1)
                .print();
        environment.execute();
    }
}

