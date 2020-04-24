package com.ns.bdp.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SourceString implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int i = 0;
        Random r = new Random();

        while (true) {
            i++;

            String name = "name" + r.nextInt(10);
            String sex = r.nextInt(10) % 2 == 0 ? "male" : "female";
            long scoreTime = System.currentTimeMillis();

            Thread.sleep(1000);
            sourceContext.collect(scoreTime + "," + name + "," + sex);
        }
    }

    @Override
    public void cancel() {

    }
}
