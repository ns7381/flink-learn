package com.ns.bdp.flink.hbase;


import com.ns.bdp.flink.hbase.hbase.Record;
import com.ns.bdp.flink.hbase.operators.HbaseSinkFunction;
import com.ns.bdp.flink.hbase.operators.TimeMillisSource;
import com.ns.bdp.flink.hbase.utils.EnvUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HbaseProducerMain {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = EnvUtil.getEnv();
        DataStream<Record> result = env.addSource(new TimeMillisSource()).name("TimeMillisSource");

        result.addSink(new HbaseSinkFunction()).name("sink");
        env.execute();
    }
}
