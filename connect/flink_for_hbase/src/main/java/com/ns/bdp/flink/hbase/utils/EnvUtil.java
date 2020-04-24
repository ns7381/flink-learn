package com.ns.bdp.flink.hbase.utils;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvUtil {

    public static StreamExecutionEnvironment getEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint间隔和模式
        env.enableCheckpointing(300_000, CheckpointingMode.AT_LEAST_ONCE);
        //设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(600_000);
        return env;
    }
}
