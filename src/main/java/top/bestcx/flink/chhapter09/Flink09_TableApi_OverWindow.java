package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
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
public class Flink09_TableApi_OverWindow {

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


        Table select = table.window(
                Over.partitionBy($("id"))
                        .orderBy($("ts"))
                        .preceding(UNBOUNDED_ROW)
                        .following(CURRENT_ROW)
                        .as("ow")
        ).select($("id"), $("id").count().over($("ow")));

        tableEnv.toRetractStream(select,Row.class).print();
        env.execute();
    }
}
