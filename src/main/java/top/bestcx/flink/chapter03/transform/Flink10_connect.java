package top.bestcx.flink.chapter03.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink10_connect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(10, 20, 3, 22,11,20);
        DataStreamSource<String> source2 = env.fromElements("a","b","c","e");
        source1
                .connect(source2)
                .map(new CoMapFunction<Integer, String, String>() {
                    @Override
                    public String map1(Integer value) throws Exception {
                        return value + "->s1";
                    }

                    @Override
                    public String map2(String value) throws Exception {
                        return value + "->s2";
                    }
                })
                .print();
        env.execute();
    }
}
