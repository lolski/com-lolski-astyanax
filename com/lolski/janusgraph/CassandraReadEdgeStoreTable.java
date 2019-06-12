package com.lolski.janusgraph;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.janusgraph.core.*;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.diskstorage.*;

import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;

import com.google.common.base.Preconditions;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;


public class CassandraReadEdgeStoreTable {
    private  StandardJanusGraphTx tx;
    private  StandardJanusGraph graph;
    private  TypeInspector typeManager;

    public void initGraphAndTransaction(StandardJanusGraph graph,StandardJanusGraphTx tx){
        this.tx = tx;
        this.graph = graph;
        this.typeManager = getTypeInspector();
    }

    public  TypeInspector getTypeInspector() {
        return tx;
    }
    private static Boolean isLoopAdded(Vertex vertex, String label) {
        Iterator<Vertex> adjacentVertices = vertex.vertices(Direction.BOTH, label);

        while (adjacentVertices.hasNext()) {
            Vertex adjacentVertex = adjacentVertices.next();

            if(adjacentVertex.equals(vertex)){
                return true;
            }
        }

        return false;
    }

    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            public boolean isSystemType(long typeId) {
                return IDManager.isSystemRelationTypeId(typeId);
            }

            public boolean isVertexExistsSystemType(long typeId) {
                return typeId == BaseKey.VertexExists.longId();
            }

            public boolean isVertexLabelSystemType(long typeId) {
                return typeId == BaseLabel.VertexLabelEdge.longId();
            }

            public boolean isTypeSystemType(long typeId) {
                return typeId == BaseKey.SchemaCategory.longId() ||
                        typeId == BaseKey.SchemaDefinitionProperty.longId() ||
                        typeId == BaseKey.SchemaDefinitionDesc.longId() ||
                        typeId == BaseKey.SchemaName.longId() ||
                        typeId == BaseLabel.SchemaDefinitionEdge.longId();
            }
        };
    }
    public VertexProperty.Cardinality getPropertyKeyCardinality(String name) {
        RelationType rt = typeManager.getRelationType(name);
        if (null == rt || !rt.isPropertyKey())
            return VertexProperty.Cardinality.single;
        PropertyKey pk = typeManager.getExistingPropertyKey(rt.longId());
        switch (pk.cardinality()) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new IllegalStateException("Unknown cardinality " + pk.cardinality());
        }
    }

    public Properties getProperties(String file) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
        } catch (IOException io) {
            io.printStackTrace();
        }
        return properties;
    }

    public static JanusGraphFactory.Builder createConfig(Properties properties) {
        JanusGraphFactory.Builder config = JanusGraphFactory.build();
        for (String key : properties.stringPropertyNames()) {
            config.set(key, properties.getProperty(key));
        }
        return config;
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
    public enum ArgsOptions {inputGraphProperty}

    public static void main(final String[] args) throws InterruptedException, BackendException, ExecutionException, UnsupportedEncodingException {

        final CassandraConnector client = new CassandraConnector();
        final String ipAddress = "127.0.0.1";
        final int port = 9162;
        client.connect(ipAddress, port);

        String inputProperty = args[ArgsOptions.inputGraphProperty.ordinal()];
        CassandraReadEdgeStoreTable cassandraReadEdgeStoreTable=new CassandraReadEdgeStoreTable();
        Properties properties = cassandraReadEdgeStoreTable.getProperties(inputProperty);
        JanusGraphFactory.Builder config = createConfig(properties);
        JanusGraph graph1 = config.open();

        IDManager idManager;
        StandardJanusGraph standardJanusGraph = (StandardJanusGraph)graph1;
        idManager = standardJanusGraph.getIDManager();
        GraphDatabaseConfiguration config2 = standardJanusGraph.getConfiguration();


        final org.janusgraph.diskstorage.configuration.Configuration configuration = config2.getConfiguration();
        final TypeInspector typeManager = (StandardJanusGraphTx)graph1.buildTransaction().readOnly().vertexCacheSize(200).start();
        final SystemTypeInspector systemTypes;

        cassandraReadEdgeStoreTable.initGraphAndTransaction(standardJanusGraph,(StandardJanusGraphTx)graph1.buildTransaction().readOnly().vertexCacheSize(200).start());

        systemTypes = cassandraReadEdgeStoreTable.getSystemTypeInspector();

        StaticBuffer st = idManager.getKey(4304);
        ByteBuffer bt = st.asByteBuffer();
        byte[] tooLong = bt.array();
        String hex = bytesToHex(tooLong);
        System.out.println(hex);

        String query;
        query = "use  your_keyspace_name;";
        client.getSession().execute(query);
        query ="SELECT * FROM edgestore;";
        ResultSet result = client.getSession().execute(query);



        Iterator<Row> itr = result.iterator();
        System.out.println(itr.hasNext());
        StaticBuffer prevkey;

        HashMap<Long,List<Entry>> hm = new HashMap< Long,List<Entry>>();
        while(itr.hasNext()){
            Row row = itr.next();

            ByteBuffer byte0 = row.getBytes("key");
            ByteBuffer byte1 = row.getBytes("column1");
            ByteBuffer byte2 = row.getBytes("value");

            StaticBuffer col1 = new StaticArrayBuffer(StaticArrayBuffer.of(byte0));
            StaticBuffer col2 = new StaticArrayBuffer(StaticArrayBuffer.of(byte1));
            StaticBuffer col3 = new StaticArrayBuffer(StaticArrayBuffer.of(byte2));

            WriteBuffer wb = new WriteByteBuffer();
            wb.putBytes(col2);
            int valuePos = wb.getPosition();
            wb.putBytes(col3);
            Entry entry = new StaticArrayEntry(wb.getStaticBuffer(),valuePos);

            long vertexId = idManager.getKeyID(col1);
            List<Entry> entryList;

            if(hm.containsKey(vertexId)){
                entryList = hm.get(vertexId);
                entryList.add(entry);
            }
            else{
                entryList = new ArrayList<Entry>();
                entryList.add(entry);
                hm.put(vertexId, entryList);
            }
        }
        System.out.format("%10s%50s%40s\n","Vertex Id","Relation Type","Value");

        for (Map.Entry<Long, List<Entry>> entry : hm.entrySet())
        {

            final long vertexId = entry.getKey();
            Iterable<Entry> entries = entry.getValue();

            for (final Entry data : entries) {
                RelationReader relationReader = standardJanusGraph.getEdgeSerializer();

                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
                if (systemTypes.isVertexLabelSystemType(relation.typeId)) {
                    long vertexLabelId = relation.getOtherVertexId();
                    VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
                }
            }

            for (final Entry data : entries) {
                try {
                    RelationReader relationReader = standardJanusGraph.getEdgeSerializer();
                    final RelationCache relation = relationReader.parseRelation(data, false, typeManager);

                    final RelationType type = typeManager.getExistingRelationType(relation.typeId);

                    // Decode and create the relation (edge or property)
                    if (type.isPropertyKey()) {
                        // Decode property
                        Object value = relation.getValue();
                        Preconditions.checkNotNull(value);
                        VertexProperty.Cardinality card = cassandraReadEdgeStoreTable.getPropertyKeyCardinality(type.name());
                        System.out.format("%10d%16s%34s%40s\n",vertexId,"Property:",type.name(),""+value);
                    } else {


                        assert type.isEdgeLabel();
                        if (relation.hasProperties()) {
                            // Load relation properties
                            for (final LongObjectCursor<Object> next : relation) {
                                assert next.value != null;
                                RelationType rt = typeManager.getExistingRelationType(next.key);
                                if (!rt.isPropertyKey()){
                                    throw new RuntimeException("Metaedges are not supported");
                                }
                            }
                        }
                        System.out.format("%10d%16s%34s%40s\n",vertexId,"Edge:",type.name(),""+relation.getOtherVertexId());
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        graph1.close();
        client.close();
    }
}