package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.AdsClickLog;
import top.bestcx.flink.bean.UserBehavior;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink06_Click {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
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
                .map( ad -> Tuple2.of(Tuple2.of(ad.getAdId(),ad.getProvince()),1L))
                .returns(Types.TUPLE(Types.TUPLE(Types.LONG,Types.STRING),Types.LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<Long, String>, Long>,Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple2<Tuple2<Long, String>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();



        env.execute();
    }
}
