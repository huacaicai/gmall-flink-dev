package com.zhang.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 15:08
 */
public class KafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    //获取kafka 生产者source
    //FlinkKafkaSource内部通过状态保存offset实现可重置偏移量，保证一致性
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }


    //获取kafka 消费者producer,发往指定一个分区、实现精准一次2PC
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(
                DEFAULT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(topic,element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    //获取kafka 消费者producer,动态发送多个分区、实现精准一次2PC
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000L+"");
        return new FlinkKafkaProducer<T>(
                DEFAULT_TOPIC,
                serializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
