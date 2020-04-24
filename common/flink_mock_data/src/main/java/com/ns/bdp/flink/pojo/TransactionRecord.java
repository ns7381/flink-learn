package com.ns.bdp.flink.pojo;

public class TransactionRecord {
    public String ctime; // 事件时间，格式：2020-03-17 15:44:54
    public String categoryId; // 类目id
    public String shopId; // 店铺id
    public String itemId; // 商品条目id
    public double price; // 成交额

    public TransactionRecord() {
    }

    public TransactionRecord(String ctime, String categoryId, String shopId, String itemId, double price) {
        this.ctime = ctime;
        this.categoryId = categoryId;
        this.shopId = shopId;
        this.itemId = itemId;
        this.price = price;
    }

    public static TransactionRecord of(String ctime, String categoryId, String shopId, String itemId, double price) {
        return new TransactionRecord(ctime, categoryId, shopId, itemId, price);
    }

    @Override
    public String toString() {
        return "TransactionRecord{" +
                "ctime=" + ctime +
                ", categoryId='" + categoryId + '\'' +
                ", shopId='" + shopId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", price=" + price +
                '}';
    }
}
