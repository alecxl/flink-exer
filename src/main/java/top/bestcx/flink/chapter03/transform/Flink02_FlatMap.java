package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink02_FlatMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<Integer> source = env.fromElements(100, 200, 300, 222);

        source.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                Integer value1 = value * value;
                Integer value2 = value - 10;
                out.collect(value1);
                out.collect(value2);
            }
        }).print();


        source
                .flatMap((Integer value, Collector<Integer> out) -> {
                    Integer value1 = value * value;
                    Integer value2 = value - 10;
                    out.collect(value1);
                    out.collect(value2);
                })
                .returns(Types.INT)
                .print();

        env.execute();
    }
}
