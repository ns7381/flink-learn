package com.ns.bdp.flink.stream.window;

import com.ns.bdp.flink.pojo.Click;
import com.ns.bdp.flink.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;

public class SessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了处理乱序数据，采用Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了演示方便，设计了一个包含迟到数据的source，且迟到的数据导致了原本两个不同的会话合并
        DataStream<Click> dataStream = env.addSource(new ClickSource());

        dataStream
                // 水印按照当前到达的数据最大时间戳更新，即未设置允许乱序
                .assignTimestampsAndWatermarks(new MyWatermarksAssigner())
                // 按用户分组
                .keyBy("userId")
                // 设置两个数据时间之间相差两秒即属于两个不同会话
                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                // 允许相对于窗口结束时间延后4秒，且小于当前水印的迟到数据依旧加入之前窗口的计算
                .allowedLateness(Time.seconds(4))
                // 统计本会话窗口内有多少条数据
                .process(new InnerProcessWindowFunction())
                .print();
        env.execute();
    }

    public static class MyWatermarksAssigner implements AssignerWithPeriodicWatermarks<Click> {
        Long currentMaxTimestamp = 0L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 以当前流过数据中最大的时间戳更新水印，不允许乱序
            return new Watermark(currentMaxTimestamp);
        }

        @Override
        public long extractTimestamp(Click element, long previousElementTimestamp) {
            long timestamp = element.timestamp;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

    }

    public static class InnerProcessWindowFunction extends ProcessWindowFunction<Click, String, Tuple, TimeWindow> {

        @Override
        public void process(Tuple key, Context context, Iterable<Click> elements, Collector<String> out) throws Exception {
            int count = 0;
            for (Click click : elements) {
                count++;
            }
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("======================\n");
            stringBuilder.append("窗口开始时间：").append(new Timestamp(context.window().getStart())).append("\n");
            stringBuilder.append("窗口结束时间：").append(new Timestamp(context.window().getEnd())).append("\n");
            stringBuilder
                    .append("(")
                    .append((String) key.getField(0))
                    .append(":")
                    .append(count)
                    .append(")")
                    .append("\n");
            stringBuilder.append("======================\n\n");
            out.collect(stringBuilder.toString());
        }
    }
}
