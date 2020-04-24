package com.ns.bdp.flink.hbase;


import com.ns.bdp.flink.hbase.hbase.Record;
import com.ns.bdp.flink.hbase.operators.HbaseSourceFunction;
import com.ns.bdp.flink.hbase.operators.MapRecordToString;
import com.ns.bdp.flink.hbase.operators.SinkStr;
import com.ns.bdp.flink.hbase.utils.EnvUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HbaseConsumerMain {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = EnvUtil.getEnv();

        SingleOutputStreamOperator<Record> source = env.addSource(new HbaseSourceFunction())
                .name("HbaseSource")
                .setParallelism(1);

        SingleOutputStreamOperator<String> map = source.map(new MapRecordToString())
                .name("map")
                .setParallelism(1);

        map.addSink(new SinkStr())
                .name("SinkStr")
                .setParallelism(1);

        env.execute();
    }
}
