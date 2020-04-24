package com.ns.bdp.flink.hbase.operators;

import com.ns.bdp.flink.hbase.hbase.HbaseUtil;
import com.ns.bdp.flink.hbase.hbase.Record;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseSinkFunction extends RichSinkFunction<Record> {

    private static Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        table = HbaseUtil.getTable();
    }

    @Override
    public void invoke(Record value, Context context) throws Exception {
        if (table == null) {
            table = HbaseUtil.getTable();
        }

        // number will be \x00\x00\x00\x00\x00\x00&\xD8 when not convert to string
        Put put = new Put(Bytes.toBytes(String.valueOf(value.getId())));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("timeStamp"), Bytes.toBytes(String.valueOf(value.getTimestamp())));
        table.put(put);
    }

    @Override
    public void close() throws Exception {
        super.close();
        table.close();
        HbaseUtil.close();
    }
}
