package com.ns.bdp.flink.source;

import com.ns.bdp.flink.pojo.OrderSubmitting;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class OrderSubmittingSource implements SourceFunction<OrderSubmitting> {
    private boolean isRunning = true;
    private Random random = new Random();
    private String baseIp = "101.133.138.";


    @Override
    public void run(SourceContext<OrderSubmitting> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(newOrderSubmitting());
            Thread.sleep(100);
        }
    }

    private OrderSubmitting newOrderSubmitting() {
        String userId = "user" + random.nextInt(100);
        String itemId = "item" + random.nextInt(20);
        String ip = baseIp + (1 + random.nextInt(255));
        long currentTimestamp = System.currentTimeMillis();
        return new OrderSubmitting(userId, itemId, ip, currentTimestamp);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
