package com.ns.bdp.flink.pojo;

public class OrderSubmitting {
    public String userId;
    public String itemId;
    public String ip;
    public long timestamp;

    public OrderSubmitting() {
    }

    public OrderSubmitting(String userId, String itemId, String ip, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.ip = ip;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderSubmitting{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", ip='" + ip + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
