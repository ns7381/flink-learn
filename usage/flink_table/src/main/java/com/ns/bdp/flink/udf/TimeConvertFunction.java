package com.ns.bdp.flink.udf;


import com.ns.bdp.flink.util.DateUtilss;
import org.apache.flink.table.functions.ScalarFunction;

public class TimeConvertFunction extends ScalarFunction {
    private String pattern = "yyyy-MM-dd HH:mm:ss";

    public TimeConvertFunction(String pattern) {
        this.pattern = pattern;
    }

    public Long eval(String date) {
        return DateUtilss.transDateToString(date, pattern);
    }
}
