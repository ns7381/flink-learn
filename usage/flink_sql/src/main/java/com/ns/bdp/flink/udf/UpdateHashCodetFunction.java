package com.ns.bdp.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 更新字符串的HashCode值。
 */
public class UpdateHashCodetFunction extends ScalarFunction {
    private int factor = 12;

    public UpdateHashCodetFunction() {
    }

    public UpdateHashCodetFunction(int factor) {
        this.factor = factor;
    }

    public int eval(String str) {
        return Math.abs(str.hashCode() % factor);
    }
}
