package top.bestcx.flink.chapter03.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.WaterSensor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: 曹旭
 * @date: 2020/12/18 1:06 下午
 * @description:
 */
public class Flink14_Process {
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
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    Map<String,Integer> sumMap = new HashMap<>();
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
                        String key = ctx.getCurrentKey();
                        Integer sumStateByKey = sumMap.get(key);
                        if(sumStateByKey == null){
                            sumMap.put(key,value.getVc());
                        }else {
                            sumMap.put(key,sumStateByKey + value.getVc());
                        }
                        out.collect(sumMap.get(key));
                    }
                })
                .print();

        environment.execute();
    }
}

