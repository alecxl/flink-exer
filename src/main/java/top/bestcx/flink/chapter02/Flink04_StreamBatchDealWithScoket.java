package top.bestcx.flink.chapter02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
public class Flink04_StreamBatchDealWithScoket {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> input = environment.socketTextStream("hadoop102",9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = input.flatMap((String s, Collector<Tuple2<String, Long>> collector) -> {
            String[] split = s.split(" ");
            for (String s1 : split) {
                collector.collect(Tuple2.of(s1, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));


        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = returns.keyBy(stringLongTuple2 -> {
            return stringLongTuple2.f0;
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();
//                .sum(1)
//                .print();
        environment.execute();
    }
}
