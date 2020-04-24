package com.ns.bdp.flink.pojo;

public class UserInfo {
    public long ts;
    public String name;
    public String sex;

    public UserInfo() {
    }

    public UserInfo(long ts, String name, String sex) {
        this.ts = ts;
        this.name = name;
        this.sex = sex;
    }

    public static UserInfo of(long ts, String name, String sex) {
        return new UserInfo(ts, name, sex);
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "ts=" + ts +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                '}';
    }
}
