package com.gremlinServerExample;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class GremlinTransactionVertex {

    TinkerConnector connector = TinkerConnector.getInstance();

    public void runGraph1() {
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            Client client = cluster.connect();
            Map<String, Object> params = new HashMap<>();
            params.put("name", "b1");
            List<Result> list = client.submit("g.V().has('b','name',name).values()", params).all().get();
            System.out.println(list);
        } catch (Exception ex) {
            System.out.println("Could not run query");
            ex.printStackTrace();
        }
        connector.closeCluster(cluster);
    }

    public void runGraph2() {
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            GraphTraversal<Vertex, Map<Object, Object>> tr = g.V().emit(cyclicPath().or().not(both())).
                    repeat(__.where(without("a")).store("a").both()).until(cyclicPath()).
                    group().by(path().unfold().limit(1)).
                    by(path().unfold().dedup().fold());

            /*GraphTraversal<Vertex, Map<Object, Object>> tr = g.V().emit(simplePath().or().not(both())).
                    repeat(__.where(without("a")).store("a").both()).until(simplePath()).
                    group().by(path().unfold().limit(1)).
                    by(path().unfold().dedup().fold());*/

            HashMap pathMap = (HashMap)tr.next();
            Set keySet = pathMap.keySet();
            for(Object obj:keySet){
                ArrayList list = (ArrayList)pathMap.get(obj);
                ArrayList beneids = new ArrayList<>();
                ArrayList condIds = new ArrayList<>();
                ArrayList txnIds = new ArrayList<>();
                ArrayList allIds = new ArrayList<>();
                for(Object vertex:list){
                    ReferenceVertex refVertex = (ReferenceVertex)vertex;
                    //System.out.println(refVertex.id());
                    if(refVertex.get().label().equals("t")){
                        txnIds.add(refVertex.id());
                    }
                    if(refVertex.get().label().equals("b")){
                        beneids.add(refVertex.id());
                    }
                    if(refVertex.get().label().equals("c")){
                        condIds.add(refVertex.id());
                    }
                    allIds.add(refVertex.id());
                }
                System.out.println("===========Path Found: " + list);
                System.out.println("Path: transactions Ids: " + txnIds + " Conductor Ids :" + condIds +
                        " Beneficiary Ids :" + beneids);

                //iterator through all vertices in this path.
                GraphTraversal<Vertex, Map<Object, Object>> trav = g.V().hasId(within(allIds.toArray())).valueMap();

                long totalAmount = 0;
                StringBuilder pathStr = new StringBuilder();
                StringBuilder amountStr = new StringBuilder();
                while(trav.hasNext()){
                    Map<Object, Object> vertex = trav.next();
                    //calculate transaction amount totals
                    Object name = ((ArrayList)vertex.get("name")).get(0);
                    pathStr.append(name).append(",");
                    if(((ArrayList)vertex.get("type")).get(0).equals("txn")){
                        int amount = (int)((ArrayList)vertex.get("amount")).get(0);
                        totalAmount += amount;
                        amountStr.append(name).append("=").append(amount).append(",");
                    }
                }
                if(totalAmount > 10000){ //check if CTR
                    System.out.println("found CTR based on transaction totals: " + pathStr);
                }else{
                    System.out.println("no CTR based on transaction totals: " + pathStr);
                }
                System.out.println("transaction amounts: " + amountStr + " total amount: " + totalAmount);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }finally{
            connector.closeCluster(cluster);
        }
    }

    public static void main(String args[]) {
        GremlinTransactionVertex gse = new GremlinTransactionVertex();
        GraphUtils.getInstance().createGraphTransactionVertex();
        gse.runGraph2();
    }
}
