package top.bestcx.flink.chapter03.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import top.bestcx.flink.bean.WaterSensor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author: 曹旭
 * @date: 2020/12/21 2:22 下午
 * @description:
 */
public class Flink02_Source_Customer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MyFunction("hadoop102",9999)).print();
        env.execute();
    }
}

class MyFunction implements SourceFunction<WaterSensor> {

    public MyFunction(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private String host;
    private int port;
    private boolean cancel = false;
    private Socket socket;
    private BufferedReader reader;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        socket = new Socket(host, port);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        String line = reader.readLine();

        while (!cancel && line != null) {
            String[] split = line.split(",");
            ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            line = reader.readLine();
        }

    }

    @Override
    public void cancel() {
        cancel = true;
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
