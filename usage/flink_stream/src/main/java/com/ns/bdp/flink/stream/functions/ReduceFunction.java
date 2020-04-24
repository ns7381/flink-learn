package com.ns.bdp.flink.stream.functions;

import com.ns.bdp.flink.source.RandomIntegerSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStream = env.addSource(new RandomIntegerSource());
        dataStream
                .keyBy(x -> 0)
                // 设定滚动窗口大小
                .countWindow(10)
                // 注意reduce中，输入和输出的类型是一致的
                .reduce(new org.apache.flink.api.common.functions.ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                })
                .print();
        env.execute("count job");
    }
}
