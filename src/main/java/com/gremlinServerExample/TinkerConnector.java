package com.gremlinServerExample;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.driver.Cluster;

public class TinkerConnector {
    private Configuration config;
    private static TinkerConnector instance = new TinkerConnector();

    public static TinkerConnector getInstance(){
        return instance;
    }

    private TinkerConnector() {
        try {
            config = new PropertiesConfiguration("conf/application.config");
        } catch (Exception ex) {
            System.out.println("Could not load configuration");
        }
    }

    public Cluster openCluster() throws Exception {
        String remoteConfig = (String) config.getProperty("gremlin.remote.driver.clusterFile");
        return Cluster.open(remoteConfig);
    }

    public void closeCluster(Cluster cluster) {
        if(cluster != null) {
            cluster.close();
        }
    }

}
