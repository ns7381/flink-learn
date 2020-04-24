package com.ns.bdp.flink.sql;

import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.pojo.TransactionRecord;
import com.ns.bdp.flink.transformation.SplitTransactionRecordMapFunction;
import com.ns.bdp.flink.util.TransactionRecordData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Module Desc:
 * 用于统计每天从零点开始，每个种类的商品成交额。
 * 本处代码提供了两种实现方式，
 * 一种是Flink 1.9之前针对group by可能产生的数据倾斜问题使用的拆分方法sql1，
 * 另一种是Flink 1.9提供的Local-Global Agg方式sql2。
 * sql1: 针对该需求，如果使用select column_names from table_name group by column_name的方式会因某个key的数据过多产生数据倾斜问题。
 * 在这里，我们在原有聚合条件的基础上，外加mod(hash_code(FLOOR(RAND(1) * 4)),4)条件，将原先聚集在某个节点上数据分配到其他节点上。
 * 降低了堵塞节点的数据处理量。在外层select时，将各个节点上计算的临时结果进行聚合计算。
 * sql2: 在Flink 1.9中，针对给类型问题提供了Local-Global Aggregation优化措施。期望用户书写简单的SQL语句的同时，也能保证有良好的性能。
 * Local-Global通过将Group聚合划分为两个步骤以解决数据倾斜问题，即首先在本地进行聚合操作，然后再进行全局聚合操作，显著的降低了数据
 * 网络传输和状态访问成本。
 * 参考链接：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/tuning/streaming_aggregation_optimization.html#local-global-aggregation
 */
public class TotalSalesPerCategoryIdSql {
    public static final String JOB_NAME = TotalSalesPerCategoryIdSql.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance().useBlinkPlanner()
                .inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(TransactionRecordData.simulateData());
        DataStream<TransactionRecord> recordStream = lineStream.map(new SplitTransactionRecordMapFunction());

        tEnv.createTemporaryView("record", recordStream, "ctime,categoryId,shopId,itemId,price");
        // way1:
        // mod(a,b) 表示整数a除以整数b的余数
        // hash_code(a) 返回a的hashcode的绝对值
        // floor(a) 返回小于或等于a的最大整数数值，即舍去小数位的数值。
        // rand(a) 返回大于等于0小于1的DOUBLE类型随机数。
/*        String sql1 = "select cdate,categoryId,sum(category_gmv_p) as category_gmv " +
                "from ( " +
                "select date_format(ctime,'yyyyMMdd') as cdate,categoryId,sum(price) as category_gmv_p " +
                "from record " +
                "group by categoryId,mod(hash_code(FLOOR(RAND(1) * 4)),4),date_format(ctime,'yyyyMMdd')" +
                ") " +
                "group by cdate,categoryId";*/
        // way2:
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "2s"); // 默认是-1ms，根据需求和业务容忍的延迟进行设值
        conf.setString("table.exec.mini-batch.size", "5"); // 默认是-1，根据需求和业务容忍的延迟进行设值
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // TWO_PHASE即Local Aggregation和Global Aggregation

        String sql2 = "select date_format(ctime,'yyyyMMdd') as cdate,categoryId,sum(price) " +
                "from record " +
                "group by categoryId,date_format(ctime,'yyyyMMdd')";

        Table resultTable = tEnv.sqlQuery(sql2);
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(JOB_NAME);
    }
}
