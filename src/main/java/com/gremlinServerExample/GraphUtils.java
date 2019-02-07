package com.gremlinServerExample;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

class GraphUtils {

    private TinkerConnector connector = TinkerConnector.getInstance();
    private static GraphUtils instance = new GraphUtils();

    private GraphUtils(){}
    static GraphUtils getInstance(){ return instance; }

    void createGraphTransactionVertex() {
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final Vertex b1 = g.addV("b").property("name", "b1").property("type", "bene").property("btin", "b1111").next();
            final Vertex b2 = g.addV("b").property("name", "b2").property("type", "bene").property("btin", "b2222").next();
            final Vertex b3 = g.addV("b").property("name", "b3").property("type", "bene").property("btin", "b3333").next();
            final Vertex c1 = g.addV("c").property("name", "c1").property("type", "cond").property("ctin", "c1111").next();
            final Vertex c2 = g.addV("c").property("name", "c2").property("type", "cond").property("ctin", "c2222").next();
            final Vertex t1 = g.addV("t").property("name", "t1").property("type", "txn").property("amount", 6000).next();
            final Vertex t2 = g.addV("t").property("name", "t2").property("type", "txn").property("amount", 5000).next();
            final Vertex t3 = g.addV("t").property("name", "t3").property("type", "txn").property("amount", 7000).next();
            final Vertex t4 = g.addV("t").property("name", "t4").property("type", "txn").property("amount", 8000).next();

            g.addE("conducted").from(c1).to(t1).next();
            g.addE("conducted").from(c1).to(t2).next();
            g.addE("conducted").from(t1).to(b1).next();
            g.addE("conducted").from(c1).to(t3).next();
            g.addE("conducted").from(t2).to(b1).next();
            g.addE("conducted").from(t3).to(b2).next();
            g.addE("conducted").from(c2).to(t4).next();
            g.addE("conducted").from(t4).to(b3).next();

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Could not create graph");
        } finally {
            connector.closeCluster(cluster);
        }
    }

    void createGraphTransactionEdge() {
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final Vertex b1 = g.addV("b").property("name", "b1").property("type", "bene").property("btin", "b1111").next();
            final Vertex b2 = g.addV("b").property("name", "b2").property("type", "bene").property("btin", "b2222").next();
            final Vertex b3 = g.addV("b").property("name", "b3").property("type", "bene").property("btin", "b3333").next();
            final Vertex b4 = g.addV("b").property("name", "b4").property("type", "bene").property("btin", "b4444").next();
            final Vertex b5 = g.addV("b").property("name", "b5").property("type", "bene").property("btin", "b5555").next();
            final Vertex c1 = g.addV("c").property("name", "c1").property("type", "cond").property("ctin", "c1111").next();
            final Vertex c2 = g.addV("c").property("name", "c2").property("type", "cond").property("ctin", "c2222").next();
            final Vertex c3 = g.addV("c").property("name", "c3").property("type", "cond").property("ctin", "c3333").next();
            final Vertex c4 = g.addV("c").property("name", "c4").property("type", "cond").property("ctin", "c4444").next();
            final Vertex c5 = g.addV("c").property("name", "c5").property("type", "cond").property("ctin", "c5555").next();

            g.addE("transaction").from(c1).to(b1).property("name", "t1").property("amount", 6000).next();
            g.addE("transaction").from(c1).to(b1).property("name", "t2").property("amount", 5000).next();
            g.addE("transaction").from(c1).to(b2).property("name", "t3").property("amount", 7000).next();
            g.addE("transaction").from(c2).to(b3).property("name", "t4").property("amount", 8000).next();
            g.addE("transaction").from(c3).to(b4).property("name", "t5").property("amount", 11000).next();
            g.addE("transaction").from(c4).to(b4).property("name", "t6").property("amount", 3000).next();
            g.addE("transaction").from(c1).to(b5).property("name", "t7").property("amount", 3000).next();

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Could not create graph");
        } finally {
            connector.closeCluster(cluster);
        }
    }

}
