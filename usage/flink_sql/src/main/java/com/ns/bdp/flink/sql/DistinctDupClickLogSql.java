package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.Commodity;
import com.ns.bdp.flink.transformation.SplitCommodityMapFunction;
import com.ns.bdp.flink.util.CommodityData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Module Desc:
 * 对商品销售数据重复上传进行去重处理。
 * 去重规则：商品id，种类id，销售量一致时即可认为该数据重复了
 * 模拟数据格式：时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class DistinctDupClickLogSql {
    public static final String JOB_NAME = DistinctDupClickLogSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(CommodityData.simulateData());
        DataStream<Commodity> rowStream = lineStream.map(new SplitCommodityMapFunction());

        tEnv.createTemporaryView("s_clickLog", rowStream, "atime,pid,cid,sales,proctime.proctime");
        String sql = "select atime,pid,cid,sales " +
                "from (" +
                "select atime,pid,cid,sales," +
                "row_number() over (partition by pid,cid,sales order by proctime asc) as rownum " +
                "from s_clickLog) " +
                "where rownum = 1";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toRetractStream(table, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
