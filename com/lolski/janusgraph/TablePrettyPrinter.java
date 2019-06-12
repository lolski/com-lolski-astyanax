package com.lolski.janusgraph;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.nio.ByteBuffer;

public class TablePrettyPrinter {
    public static void main(String[] args) {
        String host = "localhost";
        String keyspace = args[0];

        StandardJanusGraph graph = null;
        Cluster cluster  = null;
        Session session = null;

        try {
            graph = (StandardJanusGraph) JanusGraphFactory.build().
                    set("storage.backend", "cassandra").
                    set("storage.hostname", host).
                    set("storage.cassandra.keyspace", keyspace).
                    open();
            IDManager idManager = graph.getIDManager();
            cluster = Cluster.builder().addContactPoint(host).build();
            session = cluster.connect(keyspace);

            // print edgestore
            ResultSet results = session.execute("select * from edgestore");
            for (Row result : results.all()) {
                StaticArrayBuffer key = StaticArrayBuffer.of(result.getBytes("key"));
                StaticArrayBuffer column1 = StaticArrayBuffer.of(result.getBytes("column1"));
                StaticArrayBuffer value = StaticArrayBuffer.of(result.getBytes("value"));
                System.out.println("key=[" + idManager.getKeyID(key) + "], column1=[" + column1 + "], value=[" + value + "]");
            }
        }
        finally {
            session.close();
            cluster.close();
            graph.close();
        }
    }
}