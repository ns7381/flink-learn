package com.ns.bdp.flink.table;

import com.ns.bdp.flink.pojo.Commodity;
import com.ns.bdp.flink.sink.LogSink;
import com.ns.bdp.flink.transformation.SplitCommodityMapFunction;
import com.ns.bdp.flink.udf.TopNFunction;
import com.ns.bdp.flink.util.CommodityData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计每小时每个种类销售量排在前三的商品信息
 * 模拟数据格式：时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class CommodityTop3OfCategoryPerHourTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> lineStream = env.fromCollection(CommodityData.simulateData());
        DataStream<Commodity> commodityStream = lineStream.map(new SplitCommodityMapFunction());
        DataStream<Commodity> waterDataStream = commodityStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Commodity>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Commodity element) {
                        return element.atime;
                    }
                }
        );

        tEnv.createTemporaryView("t_orders", waterDataStream, "atime,pid,cid,sales,rowtime.rowtime");

        tEnv.registerFunction("top3CommodityFunction", new TopNFunction(3));
        Table resultTable = tEnv.from("t_orders")
                .window(Tumble.over("1.hours").on("rowtime").as("w"))
                .groupBy("cid,w")
                .flatAggregate("top3CommodityFunction(atime,pid,sales) as (atime,pid,sales)")
                .select("w.start,w.end,atime,pid,cid,sales");
        tEnv.toRetractStream(resultTable, Row.class).addSink(new LogSink()).setParallelism(1);

        env.execute(CommodityTop3OfCategoryPerHourTable.class.getSimpleName());
    }
}
