package com.ns.bdp.flink.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
    private int bound;

    private static final long serialVersionUID = 1L;

    private Random rnd = new Random();

    private volatile boolean isRunning = true;
    private int counter = 0;

    public RandomFibonacciSource(int bound) {
        this.bound = bound;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

        while (isRunning) {
            int first = rnd.nextInt(bound / 2 - 1) + 1;
            int second = rnd.nextInt(bound / 2 - 1) + 1;

            ctx.collect(new Tuple2<>(first, second));
            counter++;
            Thread.sleep(50L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
