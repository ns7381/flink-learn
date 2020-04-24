package com.ns.bdp.flink.pojo;

public class Payment {
    public String orderId;
    public String payType;
    public long payTime;

    public Payment() {
    }

    public Payment(String orderId, String payType, long payTime) {
        this.orderId = orderId;
        this.payType = payType;
        this.payTime = payTime;
    }
}
