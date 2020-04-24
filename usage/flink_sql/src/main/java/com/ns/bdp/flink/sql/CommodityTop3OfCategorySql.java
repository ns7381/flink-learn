package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.pojo.Commodity;
import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.transformation.SplitCommodityMapFunction;
import com.ns.bdp.flink.util.CommodityData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计每个种类销售量排在前三的商品信息
 * 模拟数据格式：时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class CommodityTop3OfCategorySql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(CommodityData.simulateData());
        DataStream<Commodity> rowStream = lineStream.map(new SplitCommodityMapFunction());

        tEnv.createTemporaryView("t_orders", rowStream, "atime,pid,cid,sales");

        String sql = "select atime,pid,cid,sales" +
                " from (" +
                "select atime,pid,cid,sales," +
                "row_number() over (partition by cid order by sales desc) as row_num from t_orders) " +
                "where row_num <= 3";
        Table topNTable = tEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> topNStream = tEnv
                .toRetractStream(topNTable, Row.class);
        topNStream.addSink(new LogSink()).setParallelism(1);

        env.execute("sql_top3");
    }
}
