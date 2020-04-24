package com.ns.bdp.flink.util;


import java.util.Arrays;
import java.util.Collection;

public class SaleRecordData {
    public static Collection<String> simulateData() {
        String[] data = new String[]{
                "1584412240404,s1,p1,3",
                "1584412240405,s2,p1,1",
                "1584412240999,s3,p2,3",

                "1584412241000,s1,p2,2",
                "1584412241404,s3,p1,3",
                "1584412241405,s2,p2,1",
                "1584412241406,s1,p2,3",
                "1584412241407,s1,p2,3",
                "1584412241999,s3,p1,2",

                "1584412242404,s1,p1,1",
                "1584412242405,s2,p2,3",
                "1584412242406,s2,p2,2",
                "1584412242407,s3,p1,1",
                "1584412242408,s1,p2,3",
                "1584412242409,s1,p1,1",
                "1584412242410,s1,p1,1",

                "1584612242410,s1,p1,1"
        };
        return Arrays.asList(data);
    }
}
