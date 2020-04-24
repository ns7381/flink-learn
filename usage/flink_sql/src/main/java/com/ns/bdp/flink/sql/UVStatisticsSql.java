package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.UserOperRecord;
import com.ns.bdp.flink.transformation.SplitUserOperRecordMapFunction;
import com.ns.bdp.flink.util.UserOperRecordData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计从0点开始全网UV、来自手机客户端的UV、来自PC的UV的数据信息。
 */
public class UVStatisticsSql {
    public static final String JOB_NAME = UVStatisticsSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(UserOperRecordData.simulateData());
        DataStream<UserOperRecord> recordStream = lineStream.map(new SplitUserOperRecordMapFunction());

        tEnv.createTemporaryView("record", recordStream, "ctime, categoryId, shopId, itemId, uid, platform, action");

        // 注意：在数据量不大的情况下不建议使用PartialFinal优化方法。
        // PartialFinal优化会自动打散成两层聚合，引入额外的网络shuffle，在数据量不大的情况下，反而会浪费资源。
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("table.optimizer.distinct-agg.split.enabled", "true");
        conf.setString("table.optimizer.distinct-agg.split.bucket-num", "4"); // 默认值是1024，可以根据业务数据量和热点情况，设置这个值

        String sql = "select" +
                " date_format(ctime,'yyyyMMdd')as cdate," +
                " count(distinct uid) as total_uv," +
                " count(distinct uid) filter (where platform in ('android','iphone')) as app_uv," +
                " count(distinct uid) filter (where platform in ('wap','other'))as web_uv " +
                "from record " +
                "group by date_format(ctime,'yyyyMMdd')";
        Table resultTable = tEnv.sqlQuery(sql);
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
