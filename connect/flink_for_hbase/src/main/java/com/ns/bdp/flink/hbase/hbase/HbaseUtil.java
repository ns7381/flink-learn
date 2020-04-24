package com.ns.bdp.flink.hbase.hbase;

import com.ns.bdp.flink.hbase.constants.HbaseConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.concurrent.Executors;

public class HbaseUtil {

    private static Connection conn = null;

    public static Table getTable() throws Exception{
        conn = getConnectionFromPool();
        return conn.getTable(TableName.valueOf(HbaseConstants.TABLENAME));
    }

    public static void close() throws IOException {
        conn.close();
        conn = null;
    }


    public static Connection getConnectionFromPool() throws IOException {
            if (conn == null || conn.isClosed() || conn.isAborted()) {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", HbaseConstants.INSTANCE);
                conn = ConnectionFactory.createConnection(conf, Executors.newFixedThreadPool(5));
            }
        return conn;
    }


}
