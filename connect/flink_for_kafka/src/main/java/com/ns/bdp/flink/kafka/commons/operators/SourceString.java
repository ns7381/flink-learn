package com.ns.bdp.flink.kafka.commons.operators;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SourceString implements SourceFunction<String> {

    private volatile boolean isRunning = true;


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            Thread.sleep(1000);
            sourceContext.collect(System.currentTimeMillis()+"");
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
