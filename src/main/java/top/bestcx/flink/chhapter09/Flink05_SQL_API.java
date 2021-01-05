package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/29 9:05
 */
public class Flink05_SQL_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;  //使用毫秒. 返回event time
                    }
                });


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(wms);

        SingleOutputStreamOperator<WaterSensor> sensorDS1 = env
                .readTextFile("input/sensor-data.log")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(wms);

        // TODO SQL 基本使用
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("sensor", sensorDS);
        tableEnv.createTemporaryView("sensor1", sensorDS1);

        Table resultTable = tableEnv
//                .sqlQuery("select * from sensor where id = 'sensor_1' and ts > 5"); // 条件查询
//                .sqlQuery("select id,count(id) from sensor group by id"); // 分组查询
//                .sqlQuery("select * from sensor right join sensor1 on sensor.id=sensor1.id"); // 关联查询
//                .sqlQuery("select * from sensor where id not in (select id from sensor1)"); // 关联查询
                .sqlQuery("select * from sensor " +
                        "union all " +
                        "select * from sensor1"); // 合并查询

        tableEnv.toRetractStream(resultTable, Row.class).print();


        env.execute();
    }
}
/*

 */