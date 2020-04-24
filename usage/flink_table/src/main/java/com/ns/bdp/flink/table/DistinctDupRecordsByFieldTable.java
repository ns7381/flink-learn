package com.ns.bdp.flink.table;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.ConsumerRecord;
import com.ns.bdp.flink.udf.DistinctByFieldFunction;
import com.ns.bdp.flink.transformation.SplitConsumerRecordMapFunction;
import com.ns.bdp.flink.util.ConsumerRecordData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 按照去重规则，对用户消费日志重复上传进行去重。
 * 模拟数据格式：时间戳、用户id（uid）、商品id(pid)、消费额（costs）
 * 去重规则：如果用户id，商品id、消费额id与之前数据相同时，即认为重复。
 */
public class DistinctDupRecordsByFieldTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(ConsumerRecordData.simulateData());
        DataStream<ConsumerRecord> recordStream = lineStream.map(new SplitConsumerRecordMapFunction());

        DataStream<ConsumerRecord> watermarkStream = recordStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<ConsumerRecord>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(ConsumerRecord element) {
                        return element.atime;
                    }
                });

        Table recordTable = tEnv.fromDataStream(watermarkStream, "atime,uid,pid,costs,rowtime.rowtime");
        tEnv.registerFunction("distinctByField", new DistinctByFieldFunction());
        Table distinctedTable = recordTable
                .window(Tumble.over("1.seconds").on("rowtime").as("win"))
                .groupBy("win,uid,pid,costs")
                .aggregate("distinctByField(atime,uid,pid,costs) as (r_atime,r_uid,r_pid,r_costs)")
                .select("r_atime,r_uid,r_pid,r_costs");
        tEnv.toRetractStream(distinctedTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(DistinctDupRecordsByFieldTable.class.getSimpleName());
    }
}
