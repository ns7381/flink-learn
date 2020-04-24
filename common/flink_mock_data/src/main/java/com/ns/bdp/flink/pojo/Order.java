package com.ns.bdp.flink.pojo;

public class Order {
    public String orderId;
    public String itemId;
    public String userId;
    public double amount;
    public long timestamp;

    public Order() {
    }

    public Order(String orderId, String itemId, String userId, double amount, long timestamp) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
