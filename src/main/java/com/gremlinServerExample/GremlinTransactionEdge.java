package com.gremlinServerExample;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

public class GremlinTransactionEdge {

    TinkerConnector connector = TinkerConnector.getInstance();

    public void runGraph(){
        Cluster cluster = null;
        try {
            cluster = connector.openCluster();
            GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            GraphTraversal dgt = g.E().project("txnid", "amount", "connectedids").by(__.id()).by("amount").by(__.bothV().id().fold());

            ArrayList<Set> vertexIdSetList = new ArrayList<>();
            while(dgt.hasNext()) {
                HashMap lhm = (HashMap) dgt.next();//transaction and associated people
                Object txnId = lhm.get("txnid");
                ArrayList connectedIds = (ArrayList) lhm.get("connectedids");//retrieved connected ids
                System.out.println( "Txn Id: " + txnId + " Connected People Ids: " + connectedIds);
                Stack<Set> matchingSetStack = new Stack<>();
                for(Set vertexIdSet : vertexIdSetList){
                    Set tmp = new HashSet<>(connectedIds);
                    tmp.retainAll(vertexIdSet);
                    if(!tmp.isEmpty()) {//vertexIdSet and connectedIds have common ids merge is needed
                        System.out.println("found match: " + vertexIdSet + " vs " + connectedIds);
                        vertexIdSet.add(txnId);
                        vertexIdSet.addAll(connectedIds);
                        System.out.println("updated vl: " + vertexIdSet);
                        //push all matching sets to the stack for later merge
                        matchingSetStack.push(vertexIdSet);
                    }
                    //System.out.println("matching set stack: " + matchingSetStack);
                }
                if(matchingSetStack.isEmpty()){//add a new set
                    Set vertexIdSet = new HashSet<>();
                    vertexIdSet.add(lhm.get("txnid"));
                    vertexIdSet.addAll(connectedIds);
                    System.out.println("adding to vl: " + vertexIdSet);
                    vertexIdSetList.add(vertexIdSet);
                }else{//merge all matching sets
                    Set matchingSet = matchingSetStack.pop();
                    while(!matchingSetStack.isEmpty()){
                        Set nextSet = matchingSetStack.pop();
                        matchingSet.addAll(nextSet);
                        nextSet.clear();
                    }
                }
            }
            System.out.println(vertexIdSetList);
            for(Set vertexIdSet: vertexIdSetList){
                if(!vertexIdSet.isEmpty()) {//only process non empty sets
                    generateCTRs(g, new ArrayList(vertexIdSet));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Could not create graph");
        } finally {
            connector.closeCluster(cluster);
        }
    }

    public static void generateCTRs(GraphTraversalSource g, List vertexIds) {
        GraphTraversal<Edge, Map<Object, Object>> trav = g.E().hasId(within(vertexIds.toArray())).valueMap();

        long totalAmount = 0;
        StringBuilder pathStr = new StringBuilder();
        StringBuilder amountStr = new StringBuilder();
        while(trav.hasNext()){
            Map<Object, Object> vertex = trav.next();
            //calculate transaction amount totals
            Object name = vertex.get("name");
            int amount = (int)vertex.get("amount");
            totalAmount += amount;
            amountStr.append(name).append("=").append(amount).append(",");
            pathStr.append(name).append(",");
        }
        GraphTraversal<Vertex, Map<Object, Object>> trav1 = g.V().hasId(within(vertexIds.toArray())).valueMap();
        while(trav1.hasNext()){
            Map<Object, Object> vertex = trav1.next();
            //calculate transaction amount totals
            Object name = vertex.get("name");
            pathStr.append(name).append(",");
        }

        if(totalAmount > 10000){ //check if CTR
            System.out.println("===========found CTR based on transaction totals: " + pathStr);
        }else{
            System.out.println("===========no CTR based on transaction totals: " + pathStr);
        }
        System.out.println("transaction amounts: " + amountStr + " total amount: " + totalAmount);
    }

    public static void main(String args[]) {
        GremlinTransactionEdge gse = new GremlinTransactionEdge();
        GraphUtils.getInstance().createGraphTransactionEdge();
        gse.runGraph();
    }
}
