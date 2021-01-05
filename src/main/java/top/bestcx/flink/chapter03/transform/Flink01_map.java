package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink01_map {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<Integer> source = env.fromElements(100, 200, 300, 222);
        source
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }
                })
                .print();

        source.map( value -> value * value).print();
        env.execute();
    }
}
