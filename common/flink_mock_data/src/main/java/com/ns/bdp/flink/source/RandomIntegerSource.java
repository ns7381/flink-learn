package com.ns.bdp.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomIntegerSource implements SourceFunction<Integer> {
    private boolean isRunning = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(random.nextInt(1000));
            Thread.sleep(100);
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }

}
