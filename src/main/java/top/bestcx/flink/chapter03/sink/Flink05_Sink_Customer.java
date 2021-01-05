package top.bestcx.flink.chapter03.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import top.bestcx.flink.bean.WaterSensor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: 曹旭
 * @date: 2020/12/21 2:22 下午
 * @description:
 */
public class Flink05_Sink_Customer {

    public static void main(String[] args) throws Exception {

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        env
                .fromCollection(waterSensors)
                .addSink(new RichSinkFunction<WaterSensor>() {

                    private PreparedStatement ps;
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test","root","aaaaaa");
                        String sql = "insert into sensor values(?,?,?)";
                        ps = connection.prepareStatement(sql);
                    }

                    @Override
                    public void invoke(WaterSensor value, Context context) throws Exception {
                        ps.setString(1,value.getId());
                        ps.setLong(2,value.getTs());
                        ps.setLong(3,value.getVc());
                        ps.execute();
                    }


                    @Override
                    public void close() throws Exception {
                        if (ps != null) {
                            ps.close();
                        }
                        if (connection != null) {
                            connection.close();
                        }
                    }

                });

        env.execute();
    }
}

