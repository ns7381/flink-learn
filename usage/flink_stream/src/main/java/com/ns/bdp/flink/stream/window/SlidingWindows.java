package com.ns.bdp.flink.stream.window;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import com.ns.bdp.flink.pojo.OrderCountWithWindow;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SlidingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了处理乱序数据，采用Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Order> dataStream = env.addSource(new OrderSource());

        dataStream
                // 自定义水印更新逻辑，设置允许的最大迟到时间
                .assignTimestampsAndWatermarks(new MyWatermarksAssigner())
                // 按商品ID来分组，统计每个商品在本窗口内的销量
                .keyBy("itemId")
                // 按滑动窗口划分
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .process(new CountFunction())
                // 按窗口结束时间分组
                .keyBy("windowEnd")
                // 对同一窗口内的商品销量取TopN
                .process(new TopN(5))
                .print();

        env.execute("TopN job");
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

    public static class CountFunction extends ProcessWindowFunction<Order, OrderCountWithWindow, Tuple, TimeWindow> {
        @Override
        public void process(Tuple key, Context context, Iterable<Order> elements, Collector<OrderCountWithWindow> out) throws Exception {
            int count = 0;
            // 求窗口时间内的累计销量
            for (Order ignored : elements) {
                count++;
            }
            out.collect(new OrderCountWithWindow(
                    key.getField(0),
                    count,
                    context.window().getEnd()
            ));

        }
    }

    public static class TopN extends KeyedProcessFunction<Tuple, OrderCountWithWindow, String> {
        private int n;
        private ListState<OrderCountWithWindow> itemCountState;

        public TopN(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            super.open(parameters);
            ListStateDescriptor<OrderCountWithWindow> itemsStateDesc = new ListStateDescriptor<>(
                    "itemCountState",
                    TypeInformation.of(new TypeHint<OrderCountWithWindow>() {
                    }));
            itemCountState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(OrderCountWithWindow value, Context ctx, Collector<String> out) throws Exception {
            // 将订单数据缓存到状态中
            itemCountState.add(value);
            // 设置何时触发计算，这里设置成当水印超过窗口结束时间时
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ctx.getCurrentKey();
            // 获取收到的所有商品销量
            List<OrderCountWithWindow> allItemCount = new ArrayList<>();
            for (OrderCountWithWindow item : itemCountState.get()) {
                allItemCount.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemCountState.clear();
            // 按照销量量从大到小排序
            allItemCount.sort((o1, o2) -> o2.count - o1.count);
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < allItemCount.size() && i < n; i++) {
                OrderCountWithWindow currentItem = allItemCount.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  销量=").append(currentItem.count)
                        .append("\n");
            }
            result.append("====================================\n\n");
            out.collect(result.toString());
        }
    }
}
