package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: 曹旭
 * @date: 2020/12/27 4:20 下午
 * @description:
 */
public class BaseWindowFunction<KEY> extends ProcessWindowFunction<Long, Tuple,KEY, TimeWindow> {

    @Override
    public void process(KEY key, Context context, Iterable<Long> elements, Collector<Tuple> out) throws Exception {
        out.collect(Tuple.newInstance(3));
    }
}
