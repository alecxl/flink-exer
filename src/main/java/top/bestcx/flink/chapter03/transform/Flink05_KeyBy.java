package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink05_KeyBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(10, 20, 3, 22,11,20);
//        source
//                .map( value -> Tuple1.of(value))
//                .returns(Types.TUPLE(Types.INT))
//                .keyBy(0)
//                .sum(0)
//                .print();

        source
                .map( value -> Tuple2.of("a:"+value,value))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(0)
                .sum(1)
                .print();
        env.execute();
    }
}
