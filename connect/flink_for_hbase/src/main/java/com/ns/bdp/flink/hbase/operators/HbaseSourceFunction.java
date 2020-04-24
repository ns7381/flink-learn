package com.ns.bdp.flink.hbase.operators;


import com.ns.bdp.flink.hbase.hbase.HbaseUtil;
import com.ns.bdp.flink.hbase.hbase.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

@Slf4j
public class HbaseSourceFunction implements SourceFunction<Record> {
    private static Table table = null;
    private static volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
        if (table == null) {
            table = HbaseUtil.getTable();
        }
        //全表扫描
        Scan scan = new Scan();
        ResultScanner rss = table.getScanner(scan);

        Iterator it = rss.iterator();
        while (isRunning && it.hasNext()) {

            Result result = (Result) it.next();
            Record record = new Record();
            record.setId(Bytes.toInt(result.getRow()));

            for (Cell kv : result.rawCells()) {
                if ("name".equals(new String(CellUtil.cloneQualifier(kv)))) {
                    record.setName(new String(CellUtil.cloneValue(kv)));
                } else if ("timeStamp".equals(new String(CellUtil.cloneQualifier(kv)))) {
                    record.setTimestamp(Bytes.toLong(CellUtil.cloneValue(kv)));
                }
            }
            ctx.collect(record);
        }
        rss.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                log.info("table close failed", e);
            }
        }
    }
}
