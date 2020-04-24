package com.ns.bdp.flink.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtilss {
    // 将yyyy-MM-dd HH:mm:ss这样的格式转为时间戳形式
    public static Long transDateToString(String date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.parse(date, formatter);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}
