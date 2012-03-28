package com.hpcloud.mraas.app;

import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.nova.Client;

public class Destroyer extends Thread {
    public void destroy(Cluster cluster) {
        (new DestroyerRunnable(cluster)).start();
    }

    private class DestroyerRunnable extends Thread {
        private Cluster cluster;
        private Client client;

        public DestroyerRunnable(Cluster cluster) {
            this.cluster = cluster;
            this.client = new Client(cluster.getTenant(), cluster.getAccessKey(), cluster.getSecretKey());
        }

        public void run() {
            for (String id : cluster.getNodeIds().values()) client.destroyHost(id);
        }

    }
}

