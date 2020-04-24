package com.ns.bdp.flink.stream.multiStream;

import com.ns.bdp.flink.source.RandomIntegerSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitAndSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStream = env.addSource(new RandomIntegerSource());
        SplitStream<Integer> split = dataStream
                .split(new OutputSelector<Integer>() {
                    @Override
                    public Iterable<String> select(Integer value) {
                        List<String> output = new ArrayList<String>();
                        if (value % 2 == 0) {
                            output.add("even");
                        } else {
                            output.add("odd");
                        }
                        return output;
                    }
                });
        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        DataStream<Integer> all = split.select("even", "odd");
        even.print();
        odd.print();
        all.print();
        env.execute();
    }
}
