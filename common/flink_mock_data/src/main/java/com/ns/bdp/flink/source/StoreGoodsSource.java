package com.ns.bdp.flink.source;

import com.ns.bdp.flink.pojo.StoreGoods;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class StoreGoodsSource implements SourceFunction<StoreGoods> {
    private boolean isRunning = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<StoreGoods> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(newStoreGoods());
            Thread.sleep(100);
        }

    }

    private StoreGoods newStoreGoods() {
        String goodsId = "goodsId" + random.nextInt(10000);
        String goodsName = "goodsName" + random.nextInt(6);
        String storeId = "storeId" + random.nextInt(100);
        double price = random.nextDouble() * 100;
        long timestamp = System.currentTimeMillis();
        return new StoreGoods(goodsId, goodsName, storeId ,price, timestamp);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
