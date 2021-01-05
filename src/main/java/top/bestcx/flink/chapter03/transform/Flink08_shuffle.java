package top.bestcx.flink.chapter03.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink08_shuffle {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setParallelism(2);
        DataStreamSource<Integer> source = env.fromElements(100, 200, 300, 222);
//        source.print();
        source.shuffle().print();


        env.execute();
    }
}
