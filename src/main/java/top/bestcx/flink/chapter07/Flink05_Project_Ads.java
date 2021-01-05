package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.AdsClickLog;
import top.bestcx.flink.bean.HotAdCountByProvinceWithWindowEnd;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/27 3:45 下午
 * @description:
 */
public class Flink05_Project_Ads {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");
        source
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
                .keyBy(new KeySelector<AdsClickLog, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(AdsClickLog value) throws Exception {
                        return Tuple2.of(value.getProvince(), value.getAdId());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new BaseAggregateFunction<AdsClickLog>(),
                        new ProcessWindowFunction<Long, HotAdCountByProvinceWithWindowEnd, Tuple2<String, Long>, TimeWindow>() {
                            @Override
                            public void process(Tuple2<String, Long> key, Context context, Iterable<Long> elements, Collector<HotAdCountByProvinceWithWindowEnd> out) throws Exception {
                                out.collect(new HotAdCountByProvinceWithWindowEnd(key.f0, key.f1, elements.iterator().next(), context.window().getEnd()));
                            }
                        }
                )
                .keyBy(HotAdCountByProvinceWithWindowEnd::getWindowEnd)
                .process(new KeyedProcessFunction<Long, HotAdCountByProvinceWithWindowEnd, String>() {

                    ListState<HotAdCountByProvinceWithWindowEnd> dataList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dataList = getRuntimeContext().getListState(new ListStateDescriptor<HotAdCountByProvinceWithWindowEnd>("dataList", Types.POJO(HotAdCountByProvinceWithWindowEnd.class)));
                    }

                    @Override
                    public void processElement(HotAdCountByProvinceWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
                        dataList.add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 200);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<HotAdCountByProvinceWithWindowEnd> data = new ArrayList<>();
                        for (HotAdCountByProvinceWithWindowEnd element : dataList.get()) {
                            data.add(element);
                        }
                        dataList.clear();
                        data.sort(new Comparator<HotAdCountByProvinceWithWindowEnd>() {
                            @Override
                            public int compare(HotAdCountByProvinceWithWindowEnd o1, HotAdCountByProvinceWithWindowEnd o2) {
                                return Long.compare(o2.getCount(),o1.getCount());
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
