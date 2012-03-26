package com.hpcloud.mraas.resources;

import com.hpcloud.mraas.core.Cluster;
import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private final String name;
    private final String numNodes;

    public ClusterResource(String name, String numNodes) {
        this.name = name;
        this.numNodes = numNodes;
    }

    @GET
    @Timed
    public Cluster getCluster() {
        return new Cluster("foo", 2);
    }
}
