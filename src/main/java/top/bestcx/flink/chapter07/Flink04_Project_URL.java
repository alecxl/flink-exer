package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/27 4:05 下午
 * @description:
 */
public class Flink04_Project_URL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("input/apache.log")
                .map(data -> {
                    String[] split = data.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    long eventTime = sdf.parse(split[3]).getTime();
                    return new ApacheLog(
                            split[0],
                            split[1],
                            eventTime,
                            split[5],
                            split[6]
                    );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ApacheLog>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((data, ts) -> data.getEventTime()))
                .keyBy(ApacheLog::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10),Time.seconds(5)))
                .aggregate(
                        new BaseAggregateFunction<ApacheLog>(),
                        new ProcessWindowFunction<Long, Tuple3<String, Long, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Long> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                                out.collect(Tuple3.of(key,elements.iterator().next(),context.window().getEnd()));
                            }
                        }
                )
                .keyBy(new KeySelector<Tuple3<String, Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple3<String, Long, Long> value) throws Exception {
                        return value.f2;
                    }
                })
                .process(new KeyedProcessFunction<Long, Tuple3<String, Long, Long>, String>() {

                    ListState<Tuple3<String, Long, Long>> dataList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dataList = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, Long, Long>>("dataList", Types.TUPLE(Types.STRING,Types.LONG,Types.LONG)));
                    }

                    @Override
                    public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
                        dataList.add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 200 );
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        List<Tuple3<String, Long, Long>> data = new ArrayList<>();
                        for (Tuple3<String, Long, Long> elem : dataList.get()) {
                            data.add(elem);
                        }

                        dataList.clear();
                        data.sort(new Comparator<Tuple3<String, Long, Long>>() {
                            @Override
                            public int compare(Tuple3<String, Long, Long> o1, Tuple3<String, Long, Long> o2) {
                                return -Long.compare(o1.f1,o2.f1);
                            }
                        });

                        // 取前N
                        StringBuffer resultBuffer = new StringBuffer();
                        resultBuffer.append("==========================================================\n");
                        for (int i = 0; i < data.size(); i++) {
                            resultBuffer.append("Top" + (i + 1) + ":" + data.get(i) + "\n");
                        }
                        resultBuffer.append("=====================================================\n\n");

                        out.collect(resultBuffer.toString());


                    }
                })
                .print();


        env.execute();
    }
}
