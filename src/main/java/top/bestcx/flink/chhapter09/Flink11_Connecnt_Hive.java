package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import top.bestcx.flink.bean.WaterSensor;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: 曹旭
 * @date: 2020/12/29 10:02 上午
 * @description:
 */
public class Flink11_Connecnt_Hive {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String catalogName = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "/Users/mac/Desktop/bigdata-exer/flink-exer/src/main/resources";
        String version = "3.1.2";

        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(catalogName,hiveCatalog);

        tableEnv.useCatalog(catalogName);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        Table table = tableEnv.sqlQuery("select * from stu");

        tableEnv.toAppendStream(table,Row.class).print();


        env.execute();
    }
}
