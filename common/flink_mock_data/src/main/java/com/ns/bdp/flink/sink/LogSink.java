package com.ns.bdp.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink implements SinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(LogSink.class);

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        logger.info("value = " + value.toString());
    }
}
