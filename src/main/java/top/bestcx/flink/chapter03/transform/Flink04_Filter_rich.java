package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink04_Filter_rich {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(10, 28, 3, 22,11,20);

        source.filter(new RichFilterFunction<Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open方法");
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0 ;
            }

            @Override
            public void close() throws Exception {
                System.out.println("close方法");
            }
        }).print();


        env.execute();
    }
}
