package com.hpcloud.mraas.resources;

import com.hpcloud.mraas.core.Cluster;
import com.hpcloud.mraas.db.ClusterDAO;

import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/cluster/{id}")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private final ClusterDAO store;

    public ClusterResource(ClusterDAO store) {
        this.store = store;
    }

    @GET
    @Timed
    public Cluster getCluster(@PathParam("id") long id) {
        return store.findById(id);
    }
}
