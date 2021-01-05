package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.HotItemCountWithWindowEnd;
import top.bestcx.flink.bean.UserBehavior;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink03_Project_HotItem2 {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000)

                )
                .filter(behavior -> "pv".equals(behavior.getBehavior()))

                .keyBy( t -> t.getItemId())
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(3)))
                .aggregate(new MyAggregateFunction(), new MyWindowFunction())
                .keyBy(HotItemCountWithWindowEnd::getWindowEndTs)
                .process(new TopNFunction(3))
                .print();

        env.execute();
    }


    public static class MyAggregateFunction implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class MyWindowFunction extends ProcessWindowFunction<Long, HotItemCountWithWindowEnd,Long, TimeWindow>{


        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<HotItemCountWithWindowEnd> out) throws Exception {
//            System.out.println(elements.iterator().next());
            out.collect(new HotItemCountWithWindowEnd(key, elements.iterator().next(), context.window().getEnd()));
        }
    }

    public static class TopNFunction extends KeyedProcessFunction<Long, HotItemCountWithWindowEnd, String>{

        private int threshold ;
        ListState<HotItemCountWithWindowEnd> listData;

        public TopNFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listData = getRuntimeContext().getListState(new ListStateDescriptor<HotItemCountWithWindowEnd>("listData", Types.POJO(HotItemCountWithWindowEnd.class)));
        }

        @Override
        public void processElement(HotItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            listData.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 100);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<HotItemCountWithWindowEnd> datas = new ArrayList<>();
            for (HotItemCountWithWindowEnd element : listData.get()) {
                datas.add(element);
            }
            datas.sort(new Comparator<HotItemCountWithWindowEnd>() {
                @Override
                public int compare(HotItemCountWithWindowEnd o1, HotItemCountWithWindowEnd o2) {
                    return Long.compare(o1.getItemCount(),o2.getItemCount());
                }
            });
            // 考虑性能和资源，把没用的释放掉
            listData.clear();
            // sort 内的lambda替换
//            datas.sort((o1, o2) -> Long.compare(o1.getItemCount(),o2.getItemCount()));
//            datas.sort(Comparator.comparingLong(HotItemCountWithWindowEnd::getItemCount));

            StringBuffer res = new StringBuffer();
            res.append("=================================\n");
            for (int i = 0; i < Math.min(datas.size(),threshold); i++) {
                res.append("Top" + (i + 1) + ":" + datas.get(i) + "\n");
            }
            res.append("======================================================================\n\n");
            out.collect(res.toString());
        }
    }


}
