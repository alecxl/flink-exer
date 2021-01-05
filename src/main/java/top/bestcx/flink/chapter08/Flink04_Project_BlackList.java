package top.bestcx.flink.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.bestcx.flink.bean.AdsClickLog;
import top.bestcx.flink.chapter07.BaseAggregateFunction;
import top.bestcx.flink.chapter07.HotAdCountByUserWithWindowEnd;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/27 3:45 下午
 * @description:
 */
public class Flink04_Project_BlackList {


    static OutputTag<String> outputTag = new OutputTag<String>("blackList") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");
        KeyedStream<AdsClickLog, Tuple2<Long, Long>> adsKS = source
                .map(line -> {
                    String[] split = line.split(",");
                    return new AdsClickLog(Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            split[2],
                            split[3],
                            Long.valueOf(split[4])
                    );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000)
                )
                .keyBy(new KeySelector<AdsClickLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdsClickLog value) throws Exception {
                        return Tuple2.of(value.getUserId(), value.getAdId());
                    }
                });

        Pattern
                .<AdsClickLog>begin("start")
                .where(new SimpleCondition<AdsClickLog>() {
                    @Override
                    public boolean filter(AdsClickLog value) throws Exception {
                        return false;
                    }
                });

        env.execute();
    }
}
