package com.ns.bdp.flink.transformation;

import com.ns.bdp.flink.pojo.ConsumerRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class SplitConsumerRecordMapFunction implements MapFunction<String, ConsumerRecord> {

    @Override
    public ConsumerRecord map(String value) throws Exception {
        // 如果输入数据非法，业务需要根据实际情况处理
        String[] record = value.split(",");
        long time = Long.parseLong(record[0]);
        String uid = record[1];
        String pid = record[2];
        double costs = Double.parseDouble(record[3]);
        return ConsumerRecord.of(time, uid, pid, costs);
    }
}
