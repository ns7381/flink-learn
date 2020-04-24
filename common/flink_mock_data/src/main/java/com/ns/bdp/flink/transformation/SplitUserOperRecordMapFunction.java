package com.ns.bdp.flink.transformation;

import com.ns.bdp.flink.pojo.UserOperRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class SplitUserOperRecordMapFunction implements MapFunction<String, UserOperRecord> {
    @Override
    public UserOperRecord map(String value) {
        // 如果输入数据非法，业务需要根据实际情况处理
        String[] vs = value.split(",");
        String ctime = vs[0];
        String categoryId = vs[1];
        String shopId = vs[2];
        String itemId = vs[3];
        String uid = vs[4];
        String platform = vs[5];
        int action = Integer.parseInt(vs[6]);

        return UserOperRecord.of(ctime, categoryId, shopId, itemId, uid, platform, action);
    }
}
