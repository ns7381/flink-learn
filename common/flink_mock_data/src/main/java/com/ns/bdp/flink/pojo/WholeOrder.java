package com.ns.bdp.flink.pojo;

public class WholeOrder {
    public String orderId;
    public String productName;
    public long orderTime;
    public String payType;
    public long payTime;

    public WholeOrder() {
    }

    public WholeOrder(SubmitedOrder submitedOrder, Payment payment) {
        this.orderId = submitedOrder.orderId;
        this.productName = submitedOrder.productName;
        this.orderTime = submitedOrder.orderTime;
        this.payType = payment.payType;
        this.payTime = payment.payTime;
    }

    @Override
    public String toString() {
        return "WholeOrder{" +
                "orderId='" + orderId + '\'' +
                ", productName='" + productName + '\'' +
                ", orderTime=" + orderTime +
                ", payType='" + payType + '\'' +
                ", payTime=" + payTime +
                '}';
    }
}
