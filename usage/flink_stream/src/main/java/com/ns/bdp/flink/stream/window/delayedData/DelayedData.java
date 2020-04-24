package com.ns.bdp.flink.stream.window.delayedData;

import com.ns.bdp.flink.pojo.Order;
import com.ns.bdp.flink.source.OrderSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DelayedData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Order> dataStream = env.addSource(new OrderSource());
        final OutputTag<Order> lateOutPutTag = new OutputTag<Order>("late-data"){};
        SingleOutputStreamOperator<Double> result = dataStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        return element.timestamp;
                    }
                })
                .keyBy("userId")
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(lateOutPutTag)
                .process(new ProcessWindowFunction<Order, Double, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Order> elements, Collector<Double> out) throws Exception {
                        double sum = 0d;
                        for (Order order : elements) {
                            sum += order.amount;
                        }
                        out.collect(sum);
                    }
                });
        DataStream<Order> lateStream = result.getSideOutput(lateOutPutTag);
        result.print();
        lateStream.print();

        env.execute();
    }
}
