package com.ns.bdp.flink.util;

import java.util.Arrays;
import java.util.Collection;

/*
 * 数据格式：时间戳、用户id（uid）、商品id(pid)、消费额（costs）
 */
public class ConsumerRecordData {
    public static Collection<String> simulateData() {
        String[] data = new String[]{
                "1584412240404,u1,p1,3",
                "1584412240405,u2,p1,1",
                "1584412240999,u3,p2,3",

                "1584412241000,u1,p2,2",
                "1584412241404,u3,p1,3",
                "1584412241405,u2,p2,1",
                "1584412241406,u1,p2,3",
                "1584412241407,u1,p2,3",
                "1584412241999,u3,p1,2",

                "1584412242404,u1,p1,1",
                "1584412242405,u2,p2,3",
                "1584412242406,u2,p2,2",
                "1584412242407,u3,p1,1",
                "1584412242408,u1,p2,3",
                "1584412242409,u1,p1,1",
                "1584412242410,u1,p1,1"
        };
        return Arrays.asList(data);
    }
}
