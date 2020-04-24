package com.ns.bdp.flink.stream.functions;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class FoldFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了处理乱序数据，采用Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Order> dataStream = env.addSource(new OrderSource());
        dataStream
                // 自定义水印更新逻辑，设置允许的最大迟到时间
                .assignTimestampsAndWatermarks(new MyWatermarksAssigner())
                // 按用户ID来分组，统计每个用户滚动窗口内的累计消费金额
                .keyBy("userId")
                // 设定滚动窗口大小
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 允许当水印超过窗口结束时间后依然可以被加到该某个窗口的数据最大可延迟的时间
                .allowedLateness(Time.seconds(10))
                .fold(0d, new CountFunction())
                .print();
        env.execute("count job");
    }

    public static class MyWatermarksAssigner implements AssignerWithPeriodicWatermarks<Order> {
        Long currentMaxTimestamp = 0L;
        // 最大允许的乱序时间是5s
        final Long maxOutOfOrderness = 5000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 允许水印比当前最大时间戳小指定的乱序时间
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Order element, long previousElementTimestamp) {
            long timestamp = element.timestamp;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

    }

    public static class CountFunction implements org.apache.flink.api.common.functions.FoldFunction<Order, Double> {

        @Override
        public Double fold(Double acc, Order value) throws Exception {
            return acc + value.amount;
        }
    }

}
