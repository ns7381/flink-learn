package com.ns.bdp.flink.stream.multiStream.join;

import com.ns.bdp.flink.source.StoreGoodsSource;
import com.ns.bdp.flink.pojo.StoreGoods;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<StoreGoods> dataStream1 = env
                .addSource(new StoreGoodsSource())
                .assignTimestampsAndWatermarks(new WindowJoin.MyAscendingTimestampExtractor());

        DataStream<StoreGoods> dataStream2 = env
                .addSource(new StoreGoodsSource())
                .assignTimestampsAndWatermarks(new WindowJoin.MyAscendingTimestampExtractor());


        dataStream1
                .keyBy(new WindowJoin.MyKeySelector())
                .intervalJoin(dataStream2.keyBy(new WindowJoin.MyKeySelector()))
                .between(Time.milliseconds(-200), Time.milliseconds(200))
                .process(new ProcessJoinFunction<StoreGoods, StoreGoods, String>() {
                    @Override
                    public void processElement(StoreGoods left, StoreGoods right, Context ctx, Collector<String> out) throws Exception {
                        out.collect("time gap is " + (left.timestamp - right.timestamp) + " milliseconds");
                    }
                })
                .print();
        env.execute();
    }

}
