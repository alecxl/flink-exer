package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.WaterSensor;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
public class Flink12_RollingTransform {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<String> input = environment.socketTextStream("hadoop102", 9999);
        input
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .keyBy(WaterSensor::getId)
//                .sum("vc")
//                .print();
//                    .max("vc")
//                    .print();
//                .maxBy("vc")
//                .print();
            .maxBy("vc",false)
                .print();

        environment.execute();
    }
}

/**
 * sum 非key和聚合结果的属性保留第一个
 * max min 非key和聚合结果的属性保留第一个
 *
 * maxBy  非key和聚合结果的属性保留最新   第二个参数设置为true时,如果聚合结果相同保留第一个,否则保留最新
 *
 */
