package com.ns.bdp.flink.transformation;

import com.ns.bdp.flink.pojo.TransactionRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class SplitTransactionRecordMapFunction implements MapFunction<String, TransactionRecord> {
    @Override
    public TransactionRecord map(String value) throws Exception {
        // 如果输入数据非法，业务需要根据实际情况处理
        String[] vs = value.split(",");
        String ctime = vs[0];
        String categoryId = vs[1];
        String shopId = vs[2];
        String itemId = vs[3];
        double price = Double.parseDouble(vs[4]);

        return TransactionRecord.of(ctime, categoryId, shopId, itemId, price);
    }
}
