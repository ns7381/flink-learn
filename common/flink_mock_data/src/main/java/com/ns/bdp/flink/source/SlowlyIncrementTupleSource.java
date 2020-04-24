package com.ns.bdp.flink.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 每隔500毫秒产生一个二元组，第一个键持续弟递增，第二个键为20以内的随机整数
 */
public class SlowlyIncrementTupleSource implements SourceFunction<Tuple2<Integer, Integer>> {
    private boolean isRunning = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            ctx.collect(Tuple2.of(count, random.nextInt(20)));
            count++;
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
