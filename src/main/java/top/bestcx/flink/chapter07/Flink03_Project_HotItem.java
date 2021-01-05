package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.UserBehavior;

import java.time.Duration;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink03_Project_HotItem {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
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
                .keyBy( UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(3)))
                .process(new ProcessWindowFunction<UserBehavior, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        Long count = 0L;
                        for (UserBehavior element : elements) {
                            count++;
                        }
                        out.collect("key = " + key + " ,counts = " + count);
                    }
                })
                .print();

        env.execute();
    }
}
