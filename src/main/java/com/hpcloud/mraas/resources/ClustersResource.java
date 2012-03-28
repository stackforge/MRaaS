package com.hpcloud.mraas.resources;

import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.persistence.ClusterDAO;
import com.hpcloud.mraas.app.Provisioner;

import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/clusters")
@Produces(MediaType.APPLICATION_JSON)
public class ClustersResource {
    private final ClusterDAO store;
    private final Provisioner provisioner;

    public ClustersResource(ClusterDAO store, Provisioner provisioner) {
        this.store = store;
        this.provisioner = provisioner;
    }

    @GET
    @Timed
    public List<Cluster> getClusters() {
        return store.findAll();
    }


    @POST
    @Timed
    public Cluster createCluster(Cluster cluster) {
        final long clusterId = store.create(cluster);
        Cluster newCluster = store.findById(clusterId);
        provisioner.provision(newCluster, store);
        return newCluster;
    }
}
