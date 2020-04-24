package com.ns.bdp.flink.kafka;


import com.ns.bdp.flink.kafka.commons.EnvUtil;
import com.ns.bdp.flink.kafka.commons.operators.SinkString;
import com.ns.bdp.flink.kafka.conf.KafkaConf;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = EnvUtil.getEnv();

        // kafka 连接参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConf.ADDRESS);

        // 需指定group.id，值是管理端的app
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConf.APP);

        // 需指定client.id，值是管理端的app
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConf.APP);

        // 其他配置根据实际情况配置，这里只是演示
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Flink kafka 连接器
        // Kafka partition动态发现： 在构建 FlinkKafkaConsumer 时的 properties 中设置 flink.partition-discovery.interval-millis 参数为非负值，表示开启动态发现的开关，以及设置的时间间隔。
        // Kafka topic动态发现： 在构建 FlinkKafkaConsumer 时，topic 的描述可以传一个正则表达式描述的 pattern，比如： Patter.compile("topoc[0-9]")
        FlinkKafkaConsumer<String> consumerInstance = new FlinkKafkaConsumer<>(KafkaConf.TOPIC, new SimpleStringSchema(), props);
        // 因为 Flink 框架有容错机制，如果作业故障，如果作业开启 checkpoint，会从上一次 checkpoint 状态开始恢复。
        // 或者在停止作业的时候主动做 savepoint，启动作业时从 savepoint 开始恢复。
        // 这两种情况下恢复作业时，作业消费起始位置是从之前保存的状态中恢复，与上面提到跟 kafka 这些单独的配置无关。
        consumerInstance.setStartFromGroupOffsets();// 默认的策略，从 group offset 位置读取数据
        // 如果开启 checkpoint，这个时候作业消费的 offset 是 Flink 在 state 中自己管理和容错。
        // 此时提交 offset 到 kafka，一般都是作为外部进度的监控，想实时知道作业消费的位置和 lag 情况。
        // 此时需要 setCommitOffsetsOnCheckpoints 为 true 来设置当 checkpoint 成功时提交 offset 到 kafka。
        consumerInstance.setCommitOffsetsOnCheckpoints(true);


        SingleOutputStreamOperator<String> source = env.addSource(consumerInstance).name("kafka-source");

        source.addSink(new SinkString()).name("SinkStr");

        env.execute();
    }
}
