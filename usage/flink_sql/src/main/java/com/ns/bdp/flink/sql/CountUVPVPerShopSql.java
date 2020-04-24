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
 * Module Desc:
 * 用于统计每天从零点开始,每个店铺的uv/pv
 * 本处代码提供了两种实现方式，
 * 一种是Flink 1.9之前针对group by + 单Distinct问题可能产生的数据倾斜问题使用的拆分方法；
 * 另一种是Flink 1.9针对group by + 单Distinct问题提供的Partial-Final Agg方式。
 * sql1: 针对该需求，如果我们直接使用select count(distinct column_name) from table_name group by column_name这样的语句
 * 很可能因为数据倾斜问题导致Flink任务执行缓慢。考虑到distinct列数据可能稀散、重复度不高等问题，这里不能使用先本地聚合再全部聚合的方式。
 * 在这里，我们采用嵌套select group的方式实现功能。在内部select里，我们在原有cdate、shopId的聚合条件的基础上，加上了uid列，将原先
 * 聚集在某一台节点的数据因为uid的加入分离到多个节点上并进行distinct、count等聚合计算。在外部select里，会将各个节点上的数据进行聚合
 * 以得到最后结果。
 * sql2: 在Flink 1.9中，针对给类型问题提供了Partial-Final Aggregation优化措施。期望用户书写简单的SQL语句的同时，也能保证有良好的性能。
 * Partial-Final Aggregation优化思想是将distinct聚合操作分为两个阶段。聚合第一阶段按照聚合key和附加的桶key进行数据传输，
 * 聚合第二阶段按照指定的key将不同桶上的数据完成聚合操作。
 * 参考链接：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/tuning/streaming_aggregation_optimization.html#split-distinct-aggregation
 */
public class CountUVPVPerShopSql {
    public static final String JOB_NAME = CountUVPVPerShopSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance().inStreamingMode()
                .useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(UserOperRecordData.simulateData());
        DataStream<UserOperRecord> recordStream = lineStream.map(new SplitUserOperRecordMapFunction());
        tEnv.createTemporaryView("record", recordStream, "ctime,categoryId,shopId,itemId,uid,platform,action");

        // way1：
//        String sql1 = "select cdate,shopId," +
//                "sum(shop_uv_partial) as shop_uv," +
//                "sum(shop_pv_partial) as shop_pv " +
//                "from ( " +
//                "select date_format(ctime,'yyyyMMdd') as cdate," +
//                "shopId," +
//                "count(distinct uid) as shop_uv_partial," +
//                "count(uid) as shop_pv_partial " +
//                "from record " +
//                "group by shopId,mod(hash_code(uid),4),date_format(ctime,'yyyyMMdd')" +
//                ") " +
//                "group by cdate,shopId";

        // way2:
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("table.optimizer.distinct-agg.split.enabled", "true");
        conf.setString("table.optimizer.distinct-agg.split.bucket-num", "4"); // 默认值是1024，可以根据业务数据量和热点情况，设置这个值
        String sql2 = "select date_format(ctime,'yyyyMMdd') as cdate," +
                "shopId," +
                "count(distinct uid) as shop_uv," +
                "count(uid) as shop_pv " +
                "from record " +
                "group by date_format(ctime,'yyyyMMdd'),shopId";
        Table resultTable = tEnv.sqlQuery(sql2);
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
