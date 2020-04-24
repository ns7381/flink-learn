package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.util.CommodityData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module Desc:
 * 用于统计每分钟的订单数量。
 * 模拟数据格式：时间戳（long），用户id(String)，订单id(String)，价格（double）
 */
public class OrderNumsPerMinutesSql {
    public static final Logger logger = LoggerFactory.getLogger(OrderNumsPerMinutesSql.class);
    public static final String JOB_NAME = OrderNumsPerMinutesSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(CommodityData.simulateData());
        DataStream<Row> rowStream = lineStream.map(new MapFunction<String, Row>() {
            @Override
            // 如果输入数据非法，业务需要根据实际情况处理
            public Row map(String line) throws Exception {
                String[] splits = line.split(",");
                long time = Long.parseLong(splits[0]);
                String oid = splits[2];
                return Row.of(time, oid);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING));
        DataStream<Row> watermarkStream = rowStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(0)) {
                    @Override
                    public long extractTimestamp(Row row) {
                        return (long) row.getField(0);
                    }
                });

        tEnv.createTemporaryView("t_orders", watermarkStream, "atime,oid,rowtime.rowtime");
        String sql = "select tumble_start(rowtime,interval '1' minute) as wstart," +
                "tumble_end(rowtime,interval '1' minute) as wend," +
                "count(oid) as total " +
                "from t_orders " +
                "group by tumble(rowtime,interval '1' minute)";
        Table table = tEnv.sqlQuery(sql);
        DataStream<Row> reultStream = tEnv.toAppendStream(table, Row.class);

        reultStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                logger.info("value: " + value.toString());
            }
        }).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
