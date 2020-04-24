package com.ns.bdp.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomStringSource implements SourceFunction<String> {
    private boolean isRunning = true;
    private Random random = new Random();
    private String[] strings = {
            "a", "b", "c", "d", "e", "f", "g",
            "h", "i", "j", "k", "l", "m", "n",
            "o", "p", "q", "r", "s", "t",
            "u", "v", "w", "x", "y", "z"
    };

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(strings[random.nextInt(26)] + " " + strings[random.nextInt(26)]);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
