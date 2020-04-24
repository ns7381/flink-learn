package com.ns.bdp.flink.source;

import com.ns.bdp.flink.pojo.Order;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

/**
 * 订单mock类
 */
public class OrderSource extends RichSourceFunction<Order> {
    private boolean isRunning = true;
    private Random random = new Random();
    private long orderCount = 0L;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (isRunning) {
            orderCount++;
            ctx.collect(newOrder());
            Thread.sleep(100);
        }

    }

    private Order newOrder() {
        String orderId = "order" + orderCount;
        String itemId = "item" + random.nextInt(20);
        String userId = "user" + random.nextInt(100);
        double amount = random.nextInt(1000) * random.nextDouble();
        long currentTimestamp = System.currentTimeMillis();
        if (random.nextInt(10) < 2) {
            currentTimestamp = currentTimestamp - random.nextInt(1000) * 3;
        }
        return new Order(orderId, itemId, userId, amount, currentTimestamp);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
