package com.ns.bdp.flink.table;

import com.ns.bdp.flink.pojo.SaleRecord;
import com.ns.bdp.flink.transformation.SplitSaleRecordMapFunction;
import com.ns.bdp.flink.util.SaleRecordData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于统计每分钟的销售总额。
 * 模拟数据格式：时间戳（long），店铺id(String)，商品id(String)，销售额（double）
 */
public class OrderTotalPerMinutesTable {
    private static final Logger logger = LoggerFactory.getLogger(OrderTotalPerMinutesTable.class);
    private static final String JOB_NAME = OrderTotalPerMinutesTable.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(SaleRecordData.simulateData());
        DataStream<SaleRecord> rowStream = lineStream.map(new SplitSaleRecordMapFunction());

        DataStream<SaleRecord> watermarkStream = rowStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SaleRecord>(Time.minutes(0)) {
                    @Override
                    public long extractTimestamp(SaleRecord element) {
                        return element.atime;
                    }
                });

        Table table = tEnv.fromDataStream(watermarkStream, "atime,sales,rowtime.rowtime");
        Table orderCountTable = table
                .window(Tumble.over("60.seconds").on("rowtime").as("win"))
                .groupBy("win")
                .select("win.start,win.end,sales.sum as total");
        tEnv.toAppendStream(orderCountTable, Row.class).addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                logger.info("value = " + value.toString());
            }
        }).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
