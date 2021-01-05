package top.bestcx.flink.chapter03.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import top.bestcx.flink.bean.WaterSensor;

import java.util.Arrays;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/21 2:22 下午
 * @description:
 */
public class Flink04_Sink_ES {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        List<HttpHost> httpHost = Arrays.asList(new HttpHost("hadoop162",9200));
        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<WaterSensor>(httpHost, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                IndexRequest indexRequest = Requests.indexRequest()
                        .index("my-index")
                        .type("my-type")
                        .id(element.getId())
                        .source(JSON.toJSONString(element), XContentType.JSON);
                indexer.add(indexRequest);
            }
        });
        builder.setBulkFlushMaxActions(1);

        input
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .addSink(builder.build());
        env.execute();
    }
}

