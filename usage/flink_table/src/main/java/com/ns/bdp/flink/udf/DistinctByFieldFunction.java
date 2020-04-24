package com.ns.bdp.flink.udf;

import com.ns.bdp.flink.pojo.ConsumerRecord;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 实现按指定字段进行去重的逻辑。
 * 建议优化，当前代码用于演示。
 */
public class DistinctByFieldFunction extends AggregateFunction<Tuple4<Long, String, String, Double>, ConsumerRecord> {

    @Override
    public ConsumerRecord createAccumulator() {
        return new ConsumerRecord(null, null, null, 0);
    }

    public void accumulate(ConsumerRecord acc, long atime, String uid, String pid, double costs) {
        if (null == acc.atime) {
            acc.atime = atime;
            acc.uid = uid;
            acc.pid = pid;
            acc.costs = costs;
        }
    }

    @Override
    public Tuple4<Long, String, String, Double> getValue(ConsumerRecord acc) {
        return Tuple4.of(acc.atime, acc.uid, acc.pid, acc.costs);
    }
}
