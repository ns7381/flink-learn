package com.ns.bdp.flink.hbase.operators;

import com.ns.bdp.flink.hbase.hbase.Record;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class TimeMillisSource implements SourceFunction<Record> {

    private volatile boolean isRunning = true;
    private char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'
            , 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    private Random rnd = new Random();

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
        while (isRunning) {
            Record record = new Record();
            record.setId(rnd.nextInt(10000));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4; i++) {
                sb.append(chars[rnd.nextInt(26)]);
            }
            record.setName(sb.toString());
            record.setTimestamp(System.currentTimeMillis());

            ctx.collect(record);
            Thread.sleep(3000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
