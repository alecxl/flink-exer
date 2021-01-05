package top.bestcx.flink.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author: 曹旭
 * @date: 2020/12/27 4:20 下午
 * @description:
 */
public class BaseAggregateFunction<T> implements AggregateFunction<T,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(T value, Long accumulator) {
        return accumulator + 1;
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
