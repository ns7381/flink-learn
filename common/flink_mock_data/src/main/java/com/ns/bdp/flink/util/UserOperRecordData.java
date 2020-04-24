package com.ns.bdp.flink.util;

import java.util.Arrays;
import java.util.Collection;

public class UserOperRecordData {
    public static Collection<String> simulateData() {
        String[] data = new String[]{
                "2020-03-17 15:44:54,c01,s01,i01,u01,android,1",
                "2020-03-17 15:44:55,c02,s02,i02,u02,wap,2",
                "2020-03-17 15:44:56,c01,s03,i01,u02,iphone,0",
                "2020-03-17 15:44:57,c02,s01,i02,u01,wap,3",
                "2020-03-17 15:44:58,c01,s02,i01,u01,other,1",

                "2020-03-17 15:45:41,c01,s01,i01,u01,other,0",
                "2020-03-17 15:45:42,c02,s02,i02,u02,iphone,1",
                "2020-03-17 15:45:43,c01,s03,i01,u01,iphone,3",
                "2020-03-17 15:45:44,c02,s02,i02,u02,other,1",
                "2020-03-17 15:45:45,c01,s01,i02,u01,android,2",
                "2020-03-17 15:45:46,c02,s02,i01,u02,wap,0",
                "2020-03-17 15:45:47,c01,s01,i02,u01,iphone,1",

                "2020-03-18 12:44:54,c01,s02,i01,u01,wap,1",
                "2020-03-18 12:44:55,c02,s03,i02,u01,other,2",
                "2020-03-18 12:44:56,c01,s02,i01,u02,iphone,2",
                "2020-03-18 12:44:57,c02,s01,i02,u01,android,2",
                "2020-03-18 12:44:58,c01,s02,i01,u02,iphone,1",
                "2020-03-18 12:45:24,c01,s02,i02,u01,other,3",
                "2020-03-18 12:45:26,c01,s01,i01,u01,android,1"
        };
        return Arrays.asList(data);
    }
}
