package top.bestcx.flink.chapter03.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import top.bestcx.flink.bean.WaterSensor;

/**
 * @author: 曹旭
 * @date: 2020/12/21 2:22 下午
 * @description:
 */
public class Flink03_Sink_Redis_2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig
                .Builder()
                .setHost("hadoop162")
                .setPort(6379)
                .setMaxTotal(100)
                .setMaxIdle(100)
                .build();
        input
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .addSink( new RedisSink<>(config, new RedisMapper<WaterSensor>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.LPUSH);
                    }

                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return "sensor_list";
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }));
        env.execute();
    }
}

