package com.ns.bdp.flink.hbase.hbase;

public class Record {
    private long Id;
    private String Name;
    private long Timestamp;

    public long getId() {
        return Id;
    }

    public void setId(long id) {
        Id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(long timestamp) {
        Timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Record{" +
                "Id=" + Id +
                ", Name='" + Name + '\'' +
                ", Timestamp=" + Timestamp +
                '}';
    }
}
