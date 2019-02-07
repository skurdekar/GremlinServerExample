package com.gremlinServerExample;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class GremlinTransactionEdge2 {
    TinkerConnector connector = TinkerConnector.getInstance();

    private void runGraph() {
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            GraphTraversal dgt = g.V().hasLabel("b").inE().otherV().path();
            while(dgt.hasNext()){
                ReferencePath path = (ReferencePath)dgt.next();
                List<Object> objects = path.objects();
                ReferenceVertex bene = (ReferenceVertex)objects.get(0);
                ReferenceVertex txn = (ReferenceVertex)objects.get(1);
                ReferenceVertex cond = (ReferenceVertex)objects.get(2);
                System.out.println("hello");
            }
        }catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Could not create graph");
        } finally {
            connector.closeCluster(cluster);
        }
    }

    public static void main(String args[]) {
        GremlinTransactionEdge2 gse = new GremlinTransactionEdge2();
        //GraphUtils.getInstance().createGraphTransactionEdge();
        gse.runGraph();
    }
}
