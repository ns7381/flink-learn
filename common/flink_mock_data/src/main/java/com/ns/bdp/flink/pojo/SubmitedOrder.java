package com.ns.bdp.flink.pojo;

public class SubmitedOrder {
    public String orderId;
    public String productName;
    public long orderTime;

    public SubmitedOrder() {
    }

    public SubmitedOrder(String orderId, String productName, long orderTime) {
        this.orderId = orderId;
        this.productName = productName;
        this.orderTime = orderTime;
    }
}
