package com.ns.bdp.flink.util;


import java.util.Arrays;
import java.util.Collection;

/**
 * 商品信息：时间戳，商品id（pid），种类id（cid），销售量(sales)
 */
public class CommodityData {
    public static Collection<String> simulateData() {
        String[] data = new String[]{
                "1584412240404,p1,c1,3",
                "1584412240406,p2,c1,4",
                "1584412240407,p1,c1,3",
                "1584412240999,p3,c2,3",

                "1584412241000,p1,c2,2",
                "1584412241404,p3,c1,3",
                "1584412241405,p2,c2,1",
                "1584412241406,p1,c2,3",
                "1584412241407,p1,c2,3",
                "1584412241999,p3,c1,2",

                "1584412242404,p1,c1,1",
                "1584412242405,p2,c2,3",
                "1584412242406,p2,c2,2",
                "1584412242407,p3,c1,1",
                "1584412242408,p1,c2,3",
                "1584412242409,p1,c1,1",
                "1584412242410,p1,c1,1"
        };
        return Arrays.asList(data);
    }
}
