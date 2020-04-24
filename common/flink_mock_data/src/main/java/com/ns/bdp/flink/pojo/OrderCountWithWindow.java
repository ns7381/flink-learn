package com.ns.bdp.flink.pojo;

public class OrderCountWithWindow {
    public String itemId;
    public int count;
    public long windowEnd;

    public OrderCountWithWindow() {
    }

    public OrderCountWithWindow(String itemId, int count, long windowEnd) {
        this.itemId = itemId;
        this.count = count;
        this.windowEnd = windowEnd;
    }
}
