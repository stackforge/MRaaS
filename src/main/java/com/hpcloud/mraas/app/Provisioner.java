package com.hpcloud.mraas.app;

import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.nova.Client;
import com.hpcloud.mraas.persistence.ClusterDAO;
import java.util.HashMap;
import org.jclouds.openstack.nova.v1_1.domain.Server;


public class Provisioner extends Thread {
    public void provision(Cluster cluster, ClusterDAO store) {
        (new ProvisionerRunnable(cluster, store)).start();
    }

    private class ProvisionerRunnable extends Thread {
        private Cluster cluster;
        private Client client;
        private ClusterDAO store;

        public ProvisionerRunnable(Cluster cluster, ClusterDAO store) {
            this.cluster = cluster;
            this.store = store;
            this.client = new Client(cluster.getTenant(), cluster.getAccessKey(), cluster.getSecretKey());
            if (cluster.getNodeIds() == null) {
                cluster.setNodeIds(new HashMap<String, String>());
            }
        }

        public void run() {
            provisionNodes();
            System.out.println("......... done ........");            
        }

        private void createHost(String name) {
           Server s = client.createHost(name);
            cluster.getNodeIds().put(name, s.getId()); 
        }

        public void provisionNodes() {
            Server master = client.createHost("master");
            for (Integer i = 0; i < cluster.getNumNodes(); i++) {
                client.createHost("hadoop" + i.toString());
            }
            store.updateNodes(cluster.getId(), cluster.getNodeIds());
        }
    }
}
