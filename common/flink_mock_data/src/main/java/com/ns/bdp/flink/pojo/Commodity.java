package com.ns.bdp.flink.pojo;

/**
 * 时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class Commodity {
    public Long atime;
    public String pid;
    public String cid;
    public Integer sales;

    public Commodity() {
    }

    public Commodity(Long atime, String pid, String cid, Integer sales) {
        this.atime = atime;
        this.pid = pid;
        this.cid = cid;
        this.sales = sales;
    }

    public static Commodity of(Long atime, String pid, String cid, Integer sales) {
        return new Commodity(atime, pid, cid, sales);
    }

    @Override
    public String toString() {
        return "Commodity{" +
                "atime=" + atime +
                ", pid='" + pid + '\'' +
                ", cid='" + cid + '\'' +
                ", sales=" + sales +
                '}';
    }
}
