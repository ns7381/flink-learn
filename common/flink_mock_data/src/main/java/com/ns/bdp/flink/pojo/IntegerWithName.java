package com.ns.bdp.flink.pojo;

public class IntegerWithName {
    public String sourceName;
    public Integer value;

    public IntegerWithName() {
    }

    public IntegerWithName(String sourceName, Integer value) {
        this.sourceName = sourceName;
        this.value = value;
    }
}
