package com.ns.bdp.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class UserBehaviorAnalysis {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // 创建并连接这个 Kafka 中的 topic
        tableEnv.sqlUpdate(
                "CREATE TABLE user_behavior (\n" +
                        "    user_id BIGINT,\n" +
                        "    item_id BIGINT,\n" +
                        "    category_id BIGINT,\n" +
                        "    behavior STRING,\n" +
                        "    ts TIMESTAMP(3),\n" +
                        "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                        "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                        ") WITH (\n" +
                        "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                        "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                        "    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                        "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                        "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                        "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                        "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                        ");");
        //创建一个 ES 结果表，根据场景需求主要需要保存两个数据：小时、成交量。
        tableEnv.sqlQuery(
                "CREATE TABLE buy_cnt_per_hour ( \n" +
                        "    hour_of_day BIGINT,\n" +
                        "    buy_cnt BIGINT\n" +
                        ") WITH (\n" +
                        "    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector\n" +
                        "    'connector.version' = '6',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本\n" +
                        "    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch 地址\n" +
                        "    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch 索引名，相当于数据库的表名\n" +
                        "    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名\n" +
                        "    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新\n" +
                        "    'format.type' = 'json',  -- 输出数据格式 json\n" +
                        "    'update-mode' = 'append'\n" +
                        ");");
        //统计每小时的成交量就是每小时共有多少"buy"的用户行为
        tableEnv.sqlUpdate(
                "INSERT INTO buy_cnt_per_hour\n" +
                        "SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)\n" +
                        "FROM user_behavior\n" +
                        "WHERE behavior = 'buy'\n" +
                        "GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);");
    }
}
