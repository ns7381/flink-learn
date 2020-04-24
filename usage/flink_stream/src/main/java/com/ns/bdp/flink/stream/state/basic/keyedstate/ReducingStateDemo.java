package com.ns.bdp.flink.stream.state.basic.keyedstate;

import com.ns.bdp.flink.source.RandomLetterAndNumberSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  相比ListState，ReducingState支持增量更新
 */
public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomLetterAndNumberSource())
                .keyBy(0)
                .flatMap(new CountFunction())
                .print();
        env.execute();
    }

    public static class CountFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Integer> {
        private int count = 0;
        private transient ReducingState<Integer> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 增量更新，将旧值与新元素通过reduce函数进行迭代得到新值
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<Integer>(
                    "sum", (ReduceFunction<Integer>) Integer::sum, BasicTypeInfo.INT_TYPE_INFO);
            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
            count++;
            if (count % 1000 == 0) {
                out.collect(reducingState.get());
                reducingState.clear();
            } else {
                // add操作后自动触发reduce函数进行增量更新
                reducingState.add(value.f1);
            }
        }
    }
}
