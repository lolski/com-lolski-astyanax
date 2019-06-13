package com.lolski.janusgraph;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("Duplicates")
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
            graph.addVertex("henlo");
            List<Vertex> v = graph.traversal().V().hasLabel("henlo").toList();
            System.out.println("------ size=" + v.size() + ", id=" + v.get(0));
            // print janusgraph_ids
            ResultSet janusgraph_ids1 = session.execute("select * from janusgraph_ids");
            for (Row result : janusgraph_ids1.all()) {
                StaticArrayBuffer key = StaticArrayBuffer.of(result.getBytes("key"));
                StaticArrayBuffer column1 = StaticArrayBuffer.of(result.getBytes("column1"));
                StaticArrayBuffer value = StaticArrayBuffer.of(result.getBytes("value"));
                System.out.println("------ janusgraph_ids: key=[" + key + "], column1=[" + column1 + "], value=[" + value + "]");
            }

            // print edgestore
            ResultSet edgestore1 = session.execute("select * from edgestore");
            for (Row result : edgestore1.all()) {
                StaticArrayBuffer key = StaticArrayBuffer.of(result.getBytes("key"));
                StaticArrayBuffer column1 = StaticArrayBuffer.of(result.getBytes("column1"));
                StaticArrayBuffer value = StaticArrayBuffer.of(result.getBytes("value"));
                System.out.println("------ edgestore: key=[" + idManager.getKeyID(key) + "], column1=[" + column1 + "], value=[" + value + "]");
            }
        }
        finally {
            session.close();
            cluster.close();
            graph.close();
        }
    }
}