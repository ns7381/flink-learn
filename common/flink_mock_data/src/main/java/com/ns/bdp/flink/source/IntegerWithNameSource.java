package com.ns.bdp.flink.source;

import com.ns.bdp.flink.pojo.IntegerWithName;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class IntegerWithNameSource implements SourceFunction<IntegerWithName> {
    private String sourceName;
    private boolean isRunning = true;
    private Random random = new Random();

    public IntegerWithNameSource(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public void run(SourceContext<IntegerWithName> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(new IntegerWithName(sourceName, random.nextInt(100)));
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {

        isRunning = false;
    }
}
