package top.bestcx.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3))
 *  每次窗口的大小为5秒,但是窗口的开始为上一次起始的加3s
 */
public class Flink02_Slide_window {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.socketTextStream("hadoop102",9999)
                .flatMap(( String value, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = value.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word,1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        System.out.println(start/1000 +":" + end/1000);
                        out.collect(elements.spliterator().estimateSize() + "");
                    }
                })
                .print();

        env.execute();
    }
}


