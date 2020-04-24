package com.ns.bdp.flink.pojo;

import java.util.Objects;

public class ConsumerRecord {
    public Long atime;
    public String uid;
    public String pid;
    public double costs;

    public ConsumerRecord() {
    }

    public ConsumerRecord(Long atime, String uid, String pid, double costs) {
        this.atime = atime;
        this.uid = uid;
        this.pid = pid;
        this.costs = costs;
    }

    public static ConsumerRecord of(Long atime, String uid, String pid, double costs) {
        return new ConsumerRecord(atime, uid, pid, costs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerRecord clickLog = (ConsumerRecord) o;
        return
                Double.compare(clickLog.costs, costs) == 0 &&
                        Objects.equals(uid, clickLog.uid) &&
                        Objects.equals(pid, clickLog.pid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, pid, costs);
    }

    @Override
    public String toString() {
        return "ConsumerRecord{" +
                "atime=" + atime +
                ", uid='" + uid + '\'' +
                ", pid='" + pid + '\'' +
                ", costs=" + costs +
                '}';
    }
}
