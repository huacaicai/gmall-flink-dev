package com.zhang.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title: FlinkCDC 实时抓取数据库变化数据
 * @author: zhang
 * @date: 2022/3/10 14:00
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.配置CDC参数
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
        //TODO 3.转化为流
        env.addSource(sourceFunction).print();

        //TODO 4.执行任务
        env.execute();
    }
}
