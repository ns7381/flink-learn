package com.ns.bdp.flink.table;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.SaleRecord;
import com.ns.bdp.flink.transformation.SplitSaleRecordMapFunction;
import com.ns.bdp.flink.udf.TimeConvertFunction;
import com.ns.bdp.flink.util.SaleRecordData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计从某一个时间点开始，每家店当前销售总额、平均每单消费额、一单最大消费额、一单最小消费额等信息。
 * 模拟数据格式：时间戳、店铺id（sid）、商品id（pid）、销售额（sales）
 */
public class SaleDetailsPerShopTable {
    public static final String JOB_NAME = SaleDetailsPerShopTable.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(SaleRecordData.simulateData());
        DataStream<SaleRecord> rowStream = lineStream.map(new SplitSaleRecordMapFunction());
        DataStream<SaleRecord> watermarkStream = rowStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SaleRecord>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(SaleRecord element) {
                        return element.atime;
                    }
                });

        Table recordsTable = tEnv.fromDataStream(watermarkStream, "atime,sid,pid,sales,rowtime.rowtime");
        tEnv.registerFunction("timeConvert", new TimeConvertFunction("yyyy-MM-dd HH:mm:ss"));
        Table resultTable = recordsTable
                .filter("atime >= timeConvert('2020-03-17 10:30:40')")
                .window(Over
                        .partitionBy("sid")
                        .orderBy("rowtime")
                        .preceding("UNBOUNDED_RANGE") //对于event-time及processing-time使用unbounded_range来表示Unbounded，对于row-count使用unbounded_row来表示Unbounded
                        .following("CURRENT_RANGE")
                        .as("win"))
                .select("sid,sales.sum over win,sales.avg over win" +
                        ",sales.max over win,sales.min over win");
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
