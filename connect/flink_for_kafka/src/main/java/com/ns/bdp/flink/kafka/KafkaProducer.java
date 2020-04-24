package com.ns.bdp.flink.kafka;


import com.ns.bdp.flink.kafka.commons.EnvUtil;
import com.ns.bdp.flink.kafka.commons.operators.SourceString;
import com.ns.bdp.flink.kafka.commons.schema.ProducerStringSerializationSchema;
import com.ns.bdp.flink.kafka.conf.KafkaConf;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 *  java -cp flink_for_kafka-1.0-SNAPSHOT.jar com.jdd.bdp.flink.kafka.KafkaProducer
 *  ./bin/flink run -c com.jdd.bdp.flink.kafka.KafkaProducer /export/ns/flink_for_kafka-1.0-SNAPSHOT.jar
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getEnv();

        SingleOutputStreamOperator<String> source = env.addSource(new SourceString()).name("SourceString");

        // kafka 连接参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConf.ADDRESS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConf.APP);
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // flink kafka 连接器
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(KafkaConf.TOPIC, new ProducerStringSerializationSchema(KafkaConf.TOPIC),
                props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        boolean logFailuresOnly = false;
        boolean writeTimestampToKafka = false;
        producer.setLogFailuresOnly(logFailuresOnly);
        producer.setWriteTimestampToKafka(writeTimestampToKafka);

        source.addSink(producer).name("Kafka-Sink");

        env.execute();

    }
}
