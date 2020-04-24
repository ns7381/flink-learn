package com.ns.bdp.flink.stream.window.types;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class WinCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Order> dataStream = env.addSource(new OrderSource());
        dataStream
                .keyBy("userId")
                // 每隔5个数据划分到一个滚动窗口内
                .countWindow(5)
                .process(new InnerProcessWindowFunction())
                .print();
        env.execute();
    }

    public static class InnerProcessWindowFunction extends ProcessWindowFunction<Order, Integer, Tuple, GlobalWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Order> elements, Collector<Integer> out) throws Exception {
            int count = 0;
            // count值一定等于5
            for (Order order : elements) {
                count++;
            }
            out.collect(count);
        }
    }
}
