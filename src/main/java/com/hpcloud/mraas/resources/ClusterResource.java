package com.hpcloud.mraas.resources;

import com.hpcloud.mraas.app.Destroyer;
import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.persistence.ClusterDAO;

import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;

import javax.ws.rs.DELETE;
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
    private final Destroyer destroyer;

    public ClusterResource(ClusterDAO store, Destroyer destroyer) {
        this.store = store;
        this.destroyer = destroyer;
    }

    @GET
    @Timed
    public Cluster getCluster(@PathParam("id") long id) {
        return store.findById(id);
    }

    @DELETE
    @Timed
    public void deleteCluster(@PathParam("id") Long id) {
        Cluster cluster = store.findById(id);
        destroyer.destroy(cluster);
        System.out.println("id: " + id);
        store.deleteById(id); // TODO: move this into destroyer? mark as pending delete?
    }
}
