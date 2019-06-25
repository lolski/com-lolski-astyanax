package com.lolski.janusgraph;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("Duplicates")
public class StorageInvestigation {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        memoryDebugging(args[0]);
    }

    private static void memoryDebugging(String keyspace_) throws ExecutionException, InterruptedException {
        String host = "localhost";
        String keyspace = keyspace_;
        StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.build().
                set("storage.backend", "cassandrathrift").
                set("storage.hostname", host).
                set("storage.cassandra.keyspace", keyspace).
                open();

        ExecutorService executorService = Executors.newFixedThreadPool(36);
        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            final int i_ = i;
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                JanusGraphTransaction tx = graph.newTransaction();
                JanusGraphVertex v1 = tx.addVertex("person" + i_);
                JanusGraphVertex v2 = tx.addVertex("dog" + i_);
                v1.addEdge("has", v2);
                tx.commit();
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();

        graph.close();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }


    private static void readWriteDebugging(String keyspace_) {
        String host = "localhost";
        String keyspace = keyspace_;
        StandardJanusGraph graph = null;
        try {
            graph = (StandardJanusGraph) JanusGraphFactory.build().
                    set("storage.backend", "cassandrathrift").
                    set("storage.hostname", host).
                    set("storage.cassandra.keyspace", keyspace).
                    open();

            JanusGraphTransaction tx = graph.newTransaction();
            tx.addVertex("henlo");
            List<Vertex> v = tx.traversal().V().hasLabel("henlo").toList();
            System.out.println("------ size=" + v.size() + ", id=" + v.get(0));
            tx.commit();
        }
        finally {
            graph.close();
        }
    }

    private static void tablePrettyPrinter(String keyspace_) {
        String host = "localhost";
        String keyspace = keyspace_;

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