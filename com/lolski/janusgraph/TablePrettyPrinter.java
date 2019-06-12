package com.lolski.janusgraph;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.nio.ByteBuffer;

public class TablePrettyPrinter {
    public static void main(String[] args) {
        String keyspace = args[0];

        Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
        Session session = cluster.connect(keyspace);

        // print janusgraph_ids
        ResultSet results = session.execute("select * from janusgraph_ids");
        for (Row result: results.all()) {
            StaticArrayBuffer key = StaticArrayBuffer.of(result.getBytes("key"));
            StaticArrayBuffer column1 = StaticArrayBuffer.of(result.getBytes("column1"));
            StaticArrayBuffer value = StaticArrayBuffer.of(result.getBytes("value"));
            System.out.println("key=[" + key + "], column1=[" + column1 + "], value=[" + value + "]");
        }

        session.close();
        cluster.close();
    }
}