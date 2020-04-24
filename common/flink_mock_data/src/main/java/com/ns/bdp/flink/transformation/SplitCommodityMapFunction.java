package com.ns.bdp.flink.transformation;

import com.ns.bdp.flink.pojo.Commodity;
import org.apache.flink.api.common.functions.MapFunction;

public class SplitCommodityMapFunction implements MapFunction<String, Commodity> {
    @Override
    public Commodity map(String value) {
        // 如果输入数据非法，业务需要根据实际情况处理
        String[] vs = value.split(",");
        long time = Long.parseLong(vs[0]);
        String pid = vs[1];
        String cid = vs[2];
        int sales = Integer.parseInt(vs[3]);
        return Commodity.of(time, pid, cid, sales);
    }
}
