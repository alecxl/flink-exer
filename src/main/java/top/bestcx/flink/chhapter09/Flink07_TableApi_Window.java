package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author: 曹旭
 * @date: 2020/12/29 10:02 上午
 * @description:
 */
public class Flink07_TableApi_Window {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> sourceStream = env.readTextFile("input/sensor-data.log")
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(strategy);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(sourceStream, $("id"), $("ts").rowtime(), $("vc"));

        Table select = table
//                .window(Tumble.over(lit(3).seconds()).on("ts").as("w"))
                .window(Slide.over(lit(5).seconds()).every(lit(2).seconds()).on("ts").as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end());

        tableEnv.toRetractStream(select,Row.class).print();
        env.execute();
    }
}
