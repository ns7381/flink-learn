package com.ns.bdp.flink.pojo;

public class SaleRecord {
    public long atime;
    public String sid;
    public String pid;
    public int sales;

    public SaleRecord() {
    }

    public SaleRecord(long atime, String sid, String pid, int sales) {
        this.atime = atime;
        this.sid = sid;
        this.pid = pid;
        this.sales = sales;
    }

    public static SaleRecord of(long atime, String sid, String pid, int sales) {
        return new SaleRecord(atime, sid, pid, sales);
    }

    @Override
    public String toString() {
        return "SaleRecord{" +
                "atime=" + atime +
                ", sid='" + sid + '\'' +
                ", pid='" + pid + '\'' +
                ", sales=" + sales +
                '}';
    }
}
