package top.bestcx.flink.chhapter09;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.HotItemCountWithWindowEnd;
import top.bestcx.flink.bean.UserBehavior;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink12_HotItem_SQL {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> sourceStream = source
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000)

                );

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.createTemporaryView("userBehavior",sourceStream,$("itemId"),$("behavior"), $("timestamp").rowtime());
        Table userBehavior = tableEnv.from("userBehavior");

        Table agg = userBehavior
                .filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("timestamp")).as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"), $("itemId").count().cast(DataTypes.BIGINT()).as("itemCount"), $("w").end().as("windowEnd"));
        tableEnv.createTemporaryView("agg",agg);

        Table resultTable = tableEnv.sqlQuery("select " +
                "* " +
                "from (select " +
                "*," +
                "row_number() over(partition by windowEnd order by itemCount desc) as rownum " +
                "from agg) " +
                "where rownum <= 3");

        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
