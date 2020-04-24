package com.ns.bdp.flink.pojo;

public class Click {
    public String userId;
    public long timestamp;

    public Click() {
    }

    public Click(String userId, long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }
}
