package com.ns.bdp.flink.stream.multiStream.join;

import com.ns.bdp.flink.pojo.StoreGoods;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        List<StoreGoods> storeGoodsList1 = new ArrayList<>();
        storeGoodsList1.add(new StoreGoods("goods1", "iphone5", "store1", 4999, 1585021618540L));
        storeGoodsList1.add(new StoreGoods("goods1", "iphone6", "store2", 5399, 1585021618640L));
        storeGoodsList1.add(new StoreGoods("goods2", "iphone6", "store1", 5399, 1585021618740L));
        DataStream<StoreGoods> dataStream1 = env.fromCollection(storeGoodsList1)
                .assignTimestampsAndWatermarks(new MyAscendingTimestampExtractor());


        List<StoreGoods> storeGoodsList2 = new ArrayList<>();
        storeGoodsList2.add(new StoreGoods("goods1", "iphone5", "store5", 5299, 1585021618540L));
        storeGoodsList2.add(new StoreGoods("goods1", "iphone5", "store6", 5299, 1585021618640L));
        storeGoodsList2.add(new StoreGoods("goods2", "iphone6", "store5", 5699, 1585021618740L));
        DataStream<StoreGoods> dataStream2 = env.fromCollection(storeGoodsList2)
                .assignTimestampsAndWatermarks(new MyAscendingTimestampExtractor());

        dataStream1
                .join(dataStream2)
                .where(new MyKeySelector())
                .equalTo(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<StoreGoods, StoreGoods, String>() {
                    @Override
                    public String join(StoreGoods first, StoreGoods second) {
                        return "goodsId:" + first.goodsId + " --> " + first.storeId + " join " + second.storeId;
                    }
                })
                .print();
        env.execute();
    }

    public static class MyAscendingTimestampExtractor extends AscendingTimestampExtractor<StoreGoods> {

        @Override
        public long extractAscendingTimestamp(StoreGoods element) {
            return element.timestamp;
        }
    }

    public static class MyKeySelector implements KeySelector<StoreGoods, String> {

        @Override
        public String getKey(StoreGoods value) throws Exception {
            return value.goodsId;
        }
    }
}
