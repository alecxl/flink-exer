package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/27 3:45 下午
 * @description:
 */
public class Flink05_Project_Ads3WithBlackList {


    static OutputTag<String> outputTag = new OutputTag<String>("blackList") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");
        SingleOutputStreamOperator<AdsClickLog> black = source
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
                })
                .process(new KeyedProcessFunction<Tuple2<Long, Long>, AdsClickLog, AdsClickLog>() {

                    ValueState<Integer> adCountByUser;
                    ValueState<Boolean> isAlarm;
                    ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        adCountByUser = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("adCountByUser", Types.INT, 0));
                        isAlarm = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isAlarm", Types.BOOLEAN, false));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Types.LONG));
                    }

                    @Override
                    public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {

                        if (timerTs.value() == null) {
                            long day = ctx.timestamp() / (24 * 60 * 60 * 1000L);
                            long nextDay = day + 1;
                            long nextDayTs = nextDay * (24 * 60 * 60 * 1000L);
                            ctx.timerService().registerEventTimeTimer(nextDayTs);
                            timerTs.update(nextDayTs);
                        }


                        if (adCountByUser.value() >= 100) {
                            if (!isAlarm.value()) {
                                ctx.output(outputTag, "用户" + value.getUserId() + "对广告" + value.getAdId() + "今日点击次数达到阈值！！！");
                                isAlarm.update(true);
                            }
                        } else {
                            out.collect(value);
                            adCountByUser.update(adCountByUser.value() + 1);
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
                        adCountByUser.clear();
                        isAlarm.clear();
                        timerTs.clear();
                    }
                });
        black.getSideOutput(outputTag).print("black");


        black.keyBy(new KeySelector<AdsClickLog, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getAdId());
            }
        })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new BaseAggregateFunction<AdsClickLog>(),
                        new ProcessWindowFunction<Long, HotAdCountByUserWithWindowEnd, Tuple2<Long, Long>, TimeWindow>() {
                            @Override
                            public void process(Tuple2<Long, Long> key, Context context, Iterable<Long> elements, Collector<HotAdCountByUserWithWindowEnd> out) throws Exception {
                                out.collect(new HotAdCountByUserWithWindowEnd(key.f0, key.f1, elements.iterator().next(), context.window().getEnd()));
                            }
                        }
                )
                .keyBy(HotAdCountByUserWithWindowEnd::getWindowEnd)
                .process(new KeyedProcessFunction<Long, HotAdCountByUserWithWindowEnd, String>() {

                    ListState<HotAdCountByUserWithWindowEnd> dataList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dataList = getRuntimeContext().getListState(new ListStateDescriptor<HotAdCountByUserWithWindowEnd>("dataList", Types.POJO(HotAdCountByUserWithWindowEnd.class)));
                    }

                    @Override
                    public void processElement(HotAdCountByUserWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
                        dataList.add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 200);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<HotAdCountByUserWithWindowEnd> data = new ArrayList<>();
                        for (HotAdCountByUserWithWindowEnd element : dataList.get()) {
                            data.add(element);
                        }
                        dataList.clear();
                        data.sort(new Comparator<HotAdCountByUserWithWindowEnd>() {
                            @Override
                            public int compare(HotAdCountByUserWithWindowEnd o1, HotAdCountByUserWithWindowEnd o2) {
                                return Long.compare(o2.getCount(), o1.getCount());
                            }
                        });
                        StringBuffer resultBuffer = new StringBuffer();
                        resultBuffer.append("===================================================\n");
                        for (int i = 0; i < 3; i++) {
                            resultBuffer.append("Top" + (i + 1) + ":" + data.get(i) + "\n");
                        }
                        resultBuffer.append("===================================================\n\n");

                        out.collect(resultBuffer.toString());

                    }
                })

                .print();


        env.execute();
    }
}
