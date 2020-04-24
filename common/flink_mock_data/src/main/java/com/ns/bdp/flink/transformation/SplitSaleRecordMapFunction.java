package com.ns.bdp.flink.transformation;

import com.ns.bdp.flink.pojo.SaleRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class SplitSaleRecordMapFunction implements MapFunction<String, SaleRecord> {
    @Override
    public SaleRecord map(String value) {
        // 如果输入数据非法，业务需要根据实际情况处理
        String[] vs = value.split(",");
        long time = Long.parseLong(vs[0]);
        String sid = vs[1];
        String pid = vs[2];
        int sales = Integer.parseInt(vs[3]);
        return SaleRecord.of(time, sid, pid, sales);
    }
}
