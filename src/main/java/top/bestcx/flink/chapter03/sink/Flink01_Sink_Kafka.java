package top.bestcx.flink.chapter03.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author: 曹旭
 * @date: 2020/12/21 2:22 下午
 * @description:
 */
public class Flink01_Sink_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.socketTextStream("hadoop102",9999);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("hadoop162:9092", "topic", new SimpleStringSchema());
        input.addSink(producer);
        env.execute();
    }
}

