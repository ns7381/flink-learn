package com.ns.bdp.flink.stream.multiStream;

import com.ns.bdp.flink.source.IntegerWithNameSource;
import com.ns.bdp.flink.pojo.IntegerWithName;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<IntegerWithName> dataStream1 = env
                .addSource(new IntegerWithNameSource("source1"));

        DataStreamSource<IntegerWithName> dataStream2 = env
                .addSource(new IntegerWithNameSource("source2"));


        dataStream1
                .union(dataStream2)
                .countWindowAll(20)
                .process(new ProcessAllWindowFunction<IntegerWithName, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<IntegerWithName> elements, Collector<String> out) throws Exception {
                        int source1Count = 0;
                        int source2Count = 0;
                        for (IntegerWithName element : elements) {
                            if ("source1".equals(element.sourceName)) {
                                source1Count++;
                            } else {
                                source2Count++;
                            }
                        }
                        out.collect("source1Count: " + source1Count + ", source2Count: " + source2Count);
                    }
                })
                .print();
        env.execute();


    }
}
