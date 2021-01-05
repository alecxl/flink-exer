package top.bestcx.flink.chapter03.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink09_Split {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        /*DataStreamSource<Integer> source = env.fromElements(10, 2, 3, 40, 5);
        SplitStream<Integer> splitStream = source.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                if (value % 2 == 0) {
                    return Collections.singletonList("偶数");
                } else {
                    return Collections.singletonList("奇数");
                }

            }
        });
        splitStream
          .select("奇数")
          .print();*/
        env.execute();
    }
}
