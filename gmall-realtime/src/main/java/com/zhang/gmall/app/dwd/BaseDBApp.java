package com.zhang.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.zhang.gmall.app.func.DimSinkFunction;
import com.zhang.gmall.app.func.MyDeserializationSchema;
import com.zhang.gmall.app.func.TableProcessFunction;
import com.zhang.gmall.beans.TableProcess;
import com.zhang.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @title: 业务数据动态分流
 * @author: zhang
 * @date: 2022/3/10 14:59
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.设置检查点
       /* env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/gmall-dev"));
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        System.setProperty("HADOOP_USER_NAME", "zhang");*/

        //TODO 3.从kafka读取业务数据，转换结构，进行简单ETL
        String sourceTopic = "ods_base_db_m_2022";
        String groupId = "base_db_app_dev";
        SingleOutputStreamOperator<JSONObject> kafkaDS = env
                .addSource(KafkaUtil.getKafkaSource(sourceTopic, groupId))
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject element) throws Exception {
                        boolean flag = element.getString("table") != null &&
                                element.getString("table").length() > 0 &&
                                element.getJSONObject("data") != null &&
                                element.getJSONObject("data").size() > 0;
                        return flag;
                    }
                });

        //TODO 4.使用FlinkCDC读取配置表数据,并转化为广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop103")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall_realtime_2022")
                .tableList("gmall_realtime_2022.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        //定义广播状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state", Types.STRING, Types.POJO(TableProcess.class)
        );

        //广播配置流
        BroadcastStream<String> broadcastProcessDS = tableProcessDS.broadcast(mapStateDescriptor);
        //TODO 5.连接广播流和主流
        BroadcastConnectedStream<JSONObject, String> connectDS = kafkaDS.connect(broadcastProcessDS);
        //TODO 6.处理连接流
        //6.1定义维度数据侧输出流标签
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };
        //6.2封装处理函数
        SingleOutputStreamOperator<JSONObject> resultDS = connectDS.process(new TableProcessFunction(dimTag, mapStateDescriptor));

        //TODO 7.提取侧输出流数据,发送到hbase和kafka
        resultDS.getSideOutput(dimTag).print("dimTag");
        resultDS.getSideOutput(dimTag).addSink(new DimSinkFunction());

        resultDS.print("kafka");
        resultDS.addSink(KafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                String topic = element.getString("sinkTable");
                String data = element.getString("data");
                return new ProducerRecord<byte[], byte[]>(
                        topic,
                        data.getBytes(StandardCharsets.UTF_8)
                );
            }
        }));

        //TODO 8.执行任务
        env.execute("BaseDBApp");
    }
}
