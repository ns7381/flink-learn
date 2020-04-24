package com.ns.bdp.flink.stream.window.types;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class WinNoKeyed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Order> dataStream = env.addSource(new OrderSource());
        dataStream
                .timeWindowAll(Time.seconds(10))
                .process(new CountFunction())
                .print();
        env.execute();
    }
    public static class CountFunction extends ProcessAllWindowFunction<Order, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Order> elements, Collector<String> out) throws Exception {
            double sum = 0d;
            // 金额累加
            for (Order order : elements) {
                sum += order.amount;
            }
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(context.window().getEnd())).append("\n");
            result
                    .append("所有用户消费总金额=").append(sum)
                    .append("\n");
            result.append("====================================\n\n");
            out.collect(result.toString());

        }
    }

}
