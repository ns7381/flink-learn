package com.ns.bdp.flink.stream.window.evictors;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountEvictor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Order> dataStream = env.addSource(new OrderSource());
        dataStream
                .windowAll(GlobalWindows.create())
                .evictor(org.apache.flink.streaming.api.windowing.evictors.CountEvictor.of(10))
                .trigger(CountTrigger.of(5))
                .process(new ProcessAllWindowFunction<Order, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<Order> elements, Collector<String> out) throws Exception {
                        int count = 0;
                        double sum = 0d;
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("orderId: [");
                        for (Order order : elements) {
                            stringBuilder.append(order.orderId).append(" ");
                            count++;
                            sum += order.amount;
                        }
                        stringBuilder.append("]").append("\n");
                        stringBuilder.append("count: ").append(count).append("\n");
                        stringBuilder.append("sum: ").append(sum).append("\n");
                        stringBuilder.append("average: ").append(sum / count).append("\n\n");
                        out.collect(stringBuilder.toString());
                    }
                })
                .print();
        env.execute();
    }

}
