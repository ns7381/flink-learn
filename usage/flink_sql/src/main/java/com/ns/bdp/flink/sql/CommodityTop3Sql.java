package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.pojo.Commodity;
import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.transformation.SplitCommodityMapFunction;
import com.ns.bdp.flink.udf.UpdateHashCodetFunction;
import com.ns.bdp.flink.util.CommodityData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计全网销售量排在前三的产品信息
 * 模拟数据格式：时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class CommodityTop3Sql {
    public static final String JOB_NAME = CommodityTop3Sql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.registerFunction("getHashCode", new UpdateHashCodetFunction(4));

        DataStream<String> lineStream = env.fromCollection(CommodityData.simulateData());
        DataStream<Commodity> rowStream = lineStream.map(new SplitCommodityMapFunction());

        tEnv.createTemporaryView("s_orders", rowStream, "atime,pid,cid,sales");
        String top3PerCategorySql = "select atime,pid,cid,sales " +
                "from (" +
                "select atime,pid,cid,sales," +
                "row_number() over(partition by getHashCode(cid) order by sales desc) as rownum1 " +
                "from s_orders) " +
                "where rownum1 <= 3";
        String top3Sql = "select atime,pid,cid,sales " +
                "from (" +
                "select atime,pid,cid,sales," +
                "row_number() over(order by sales desc) as rownum2 " +
                "from top3PerCategoryTable) " +
                "where rownum2 <= 3";
        Table top3PerCategoryTable = tEnv.sqlQuery(top3PerCategorySql);
        tEnv.createTemporaryView("top3PerCategoryTable", top3PerCategoryTable);
        Table resultTable = tEnv.sqlQuery(top3Sql);
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
