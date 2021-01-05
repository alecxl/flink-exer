package top.bestcx.flink.chapter04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import top.bestcx.flink.bean.MarketingUserBehavior;
import top.bestcx.flink.bean.UserBehavior;

import java.util.*;

/**
 * @author: 曹旭
 * @date: 2020/12/21 6:22 下午
 * @description:
 */
public class Flink05_market_1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.addSource(new MySource())
                .map( behavior -> Tuple2.of(behavior.getBehavior()+ "-"+behavior.getChannel(),1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t-> t.f0)
                .sum(1)
                .print();



        env.execute();
    }
}

class MySource implements SourceFunction<MarketingUserBehavior>{

    boolean isRunning = true;
    private Random random = new Random();
    List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
    List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");


    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            long userId =  random.nextInt();
            String behavior = behaviors.get(random.nextInt(behaviors.size()));
            String channel = channels.get(random.nextInt(channels.size()));
            ctx.collect(new MarketingUserBehavior(userId,behavior,channel,System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}