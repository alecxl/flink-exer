package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.bestcx.flink.bean.WaterSensor;


/**
 * @author: 曹旭
 * @date: 2020/12/19 8:49 下午
 * @description:
 */
public class Flink06_KeyBy_02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
//        DataStreamSource<WaterSensor> source = env.fromElements
//                            (new WaterSensor("aa", 7l, 5),
//                                    new WaterSensor("aa", 10l, 5),
//                                    new WaterSensor("bb", 2l, 5));
//
//        source.keyBy("id").sum("ts").print();

        env.fromElements
                            (new WaterSensor("aa", 7l, 5),
                                    new WaterSensor("aa", 10l, 5),
                                    new WaterSensor("bb", 2l, 5))
                .keyBy("id").sum("ts").print();


        env.execute();
    }
}
