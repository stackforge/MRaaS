package com.hpcloud.mraas.core;

public class Cluster {
    private final String id;
    private final int numNodes;

    public Cluster(String id, int numNodes) {
        this.id = id;
        this.numNodes = numNodes;
    }

    public String getId() {
        return id;
    }

    public int getNumNodes() {
        return numNodes;
    }
}
