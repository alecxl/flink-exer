package top.bestcx.flink.chapter03.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.bestcx.flink.bean.WaterSensor;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
public class Flink13_Reduce {
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
//                .reduce(new ReduceFunction<WaterSensor>() {
//                    @Override
//                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                        return new WaterSensor(value1.getId(),value2.getTs(),value1.getVc() +value2.getVc());
//                    }
//                })
                .reduce( (value1,value2) -> new WaterSensor(value1.getId(),value2.getTs(),value1.getVc() +value2.getVc()))
                .print();

        environment.execute();
    }
}

