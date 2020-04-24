package com.ns.bdp.flink.stream.window;

import com.ns.bdp.flink.pojo.OrderSubmitting;
import com.ns.bdp.flink.source.OrderSubmittingSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class GlobalWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderSubmitting> dataStream = env.addSource(new OrderSubmittingSource());
        dataStream
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create())
                // 对最近两分钟的订单请求去重
                .evictor(TimeEvictor.of(Time.minutes(2)))
                .trigger(CountTrigger.of(1))
                .process(new DuplicateFilterFunction())
                .print();
        env.execute();
    }


    public static class DuplicateFilterFunction extends ProcessAllWindowFunction<OrderSubmitting, OrderSubmitting, GlobalWindow> {
        private ValueState<HashSet<String>> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<HashSet<String>> stateDescriptor = new ValueStateDescriptor<HashSet<String>>(
                    "duplicateFilter",
                    TypeInformation.of(new TypeHint<HashSet<String>>() {
                    })
            );
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void process(Context context, Iterable<OrderSubmitting> elements, Collector<OrderSubmitting> out) throws Exception {
            HashSet<String> set = valueState.value();
            if (set == null) {
                set = new HashSet<>();
            }
            for (OrderSubmitting orderSubmitting : elements) {
                if (!set.contains(orderSubmitting.ip)) {
                    set.add(orderSubmitting.ip);
                    System.out.println(set.size());
                    valueState.update(set);
                    out.collect(orderSubmitting);
                }
            }
        }
    }
}
