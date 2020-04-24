package com.ns.bdp.flink.hbase.operators;

import com.ns.bdp.flink.hbase.hbase.Record;
import org.apache.flink.api.common.functions.MapFunction;

public class MapRecordToString implements MapFunction<Record, String> {
    @Override
    public String map(Record value) throws Exception {
        return value.toString();
    }
}
