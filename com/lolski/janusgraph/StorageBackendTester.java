package com.lolski.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.TransactionBuilder;

public class StorageBackendTester {
    public static void main(String[] args) throws InterruptedException {
        test();
    }

    public static void test() {
        JanusGraphFactory.Builder graphFactory = JanusGraphFactory.build().
            set("storage.backend", "cassandra").
            set("storage.hostname", "localhost").
            set("storage.username", "cassandra").
            set("storage.password", "cassandra").
            set("storage.cassandra.keyspace", "test_keyspace");

        JanusGraph graph = graphFactory.open();
        TransactionBuilder txFactory = graph.buildTransaction().logIdentifier("test_log2");
        JanusGraphTransaction tx = txFactory.start();
        tx.addVertex("wow");
        tx.commit();
        graph.close();

    }

    public static void cassandrathrift() {
        JanusGraphFactory.Builder graphFactory = JanusGraphFactory.build().
                set("storage.hostname", "lolski-kgms-test-vm-1").
                set("storage.cassandra.keyspace", "test").
                set("storage.backend", "cassandrathrift").
                set("storage.username", "cassandra").
                set("storage.password", "cassandra").
                set("storage.connection-timeout", "100000");

        System.out.println("starting graph...");
        JanusGraph graph = graphFactory.open();
        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().withStrategies(ReadOnlyStrategy.instance()).V().has("a", "b");
        System.out.println("traversal.hasNext(): " + traversal.hasNext());
        System.out.println("closing graph...");
        graph.close();
        System.out.println("graph closed");

        System.out.println("starting graph...");
        JanusGraph graph2 = graphFactory.open();
        System.out.println("closing graph...");
        graph2.close();
        System.out.println("graph closed");
    }

    public static void cassandra() throws InterruptedException {
        System.out.println("creating graph builder...");
        JanusGraphFactory.Builder graphFactory = JanusGraphFactory.build().
                set("storage.hostname", "lolski-kgms-test-vm-1").
                set("storage.cassandra.keyspace", "test").
                set("storage.backend", "cassandra").

                /*
                 has an effect. alleviates the excerbation of timeout.
                */
                set("storage.connection-timeout", "10000");

                /*
                 these configs have a very high probability of helping with failures but they failed with exceptions
                */
                // set("storage.cassandra.astyanax.retry-policy", "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8").
                // set("storage.cassandra.astyanax.retry-backoff-strategy", "com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,10,50").

                /*
                 TODO: how to make astyanax remember if, for example, node A goes down
                */

                /*
                 the following settings didn't help: still retries a few times, each time waiting for 10 seconds
                */
                // set("storage.setup-wait", "1000"). 
                // set("storage.parallel-backend-ops", true).
                // set("storage.read-time", 1000).
                // set("storage.write-time", 1000).
                // set("storage.cassandra.astyanax.connection-pool-type", "ROUND_ROBIN") --> tried running the app with ROUND_ROBIN 4 times, absolutely no change from the original behavior (valid value: ROUND_ROBIN, TOKEN_AWARE)
                // set("storage.cassandra.astyanax.max-cluster-connections-per-host", 1).
                // set("storage.cassandra.astyanax.retry-delay-slice", "10000").
                // set("storage.cassandra.astyanax.retry-suspend-window", "10000").

        System.out.println("starting graph...");
        JanusGraph graph = graphFactory.open();
        System.out.println("closing graph...");
        graph.close();
        System.out.println("graph closed");

        System.out.println("starting graph...");
        JanusGraph graph2 = graphFactory.open();
        System.out.println("closing graph...");
        graph2.close();
        System.out.println("graph closed");
    }
}