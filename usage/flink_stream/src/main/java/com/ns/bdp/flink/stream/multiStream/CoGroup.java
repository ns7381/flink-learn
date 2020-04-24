package com.ns.bdp.flink.stream.multiStream;

import com.ns.bdp.flink.source.StoreGoodsSource;
import com.ns.bdp.flink.pojo.StoreGoods;
import com.ns.bdp.flink.stream.multiStream.join.WindowJoin;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CoGroup {
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
                .coGroup(dataStream2)
                .where(new WindowJoin.MyKeySelector())
                .equalTo(new WindowJoin.MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<StoreGoods, StoreGoods, String>() {
                    @Override
                    public void coGroup(Iterable<StoreGoods> first, Iterable<StoreGoods> second, Collector<String> out) throws Exception {
                        int firstGroupCount = 0;
                        int secondGroupCount = 0;
                        for (StoreGoods value : first) {
                            firstGroupCount++;
                        }
                        out.collect("first group count : " + firstGroupCount);
                        for (StoreGoods value : second) {
                            secondGroupCount++;
                        }
                        out.collect("second group count : " + secondGroupCount);
                    }
                })
                .print();
        env.execute();
    }

}
