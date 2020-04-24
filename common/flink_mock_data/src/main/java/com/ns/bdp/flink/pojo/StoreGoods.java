package com.ns.bdp.flink.pojo;

public class StoreGoods {
    public String goodsId;
    public String goodName;
    public String storeId;
    public double price;
    public long timestamp;

    public StoreGoods() {
    }

    public StoreGoods(String goodsId, String goodName, String storeId, double price, long timestamp) {
        this.goodsId = goodsId;
        this.goodName = goodName;
        this.storeId = storeId;
        this.price = price;
        this.timestamp = timestamp;
    }
}
