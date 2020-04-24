package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.UserInfo;
import com.ns.bdp.flink.source.SourceString;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Module Desc:
 * 在一次活动中，需要统计每分钟每隔18秒男性和女性的登录总数。
 */
public class SexCountSql {
    public static final String JOB_NAME = SexCountSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.addSource(new SourceString());
        DataStream<UserInfo> userInfoStream = lineStream.map((value) -> {
            String[] strs = value.split(",");
            return UserInfo.of(Long.parseLong(strs[0]), strs[1], strs[2]);
        });
        DataStream<UserInfo> watermarkStream = userInfoStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UserInfo>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(UserInfo element) {
                        return element.getTs();
                    }
                });

        tEnv.createTemporaryView("userinfo", watermarkStream, "ts.rowtime,name,sex");

        String sql = "select" +
                " sex," +
                " count(name) as count_name," +
                " INCREMENT_TIME() as im," +
                " INCREMENT_TRIGGER_TIME(ts, INTERVAL '1' MINUTE, INTERVAL '18' SECOND) as im1," +
                " TUMBLE_END(ts, INTERVAL '1' MINUTE, INTERVAL '18' SECOND) as tumbleend," +
                " TUMBLE_START(ts, INTERVAL '1' MINUTE, INTERVAL '18' SECOND) as tumblestart," +
                " TUMBLE_ROWTIME(ts, INTERVAL '1' MINUTE, INTERVAL '18' SECOND) as rt " +
                "from userinfo " +
                "GROUP BY sex,TUMBLE(ts, INTERVAL '1' MINUTE, INTERVAL '18' SECOND)";

        Table resultTable = tEnv.sqlQuery(sql);
        tEnv.toRetractStream(resultTable, Row.class)
                .addSink(new LogSink());

        env.execute(JOB_NAME);
    }
}
