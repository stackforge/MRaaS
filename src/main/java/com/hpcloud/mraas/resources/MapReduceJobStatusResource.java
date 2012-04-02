package com.hpcloud.mraas.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.hpcloud.mraas.domain.ClusterJob;
import com.hpcloud.mraas.persistence.MapReduceJobDAO;
import com.yammer.metrics.annotation.Timed;

@Path("/jobs/{id}")
@Produces(MediaType.APPLICATION_JSON)
public class MapReduceJobStatusResource {
    private final MapReduceJobDAO store;

    public MapReduceJobStatusResource(MapReduceJobDAO store) {
        this.store = store;
    }
    
    @GET
    @Timed
    public Integer jobStatus(@PathParam("id") long id) {
        ClusterJob job = store.findById(id);
        // TODO: query the cluster get the resource (or if the job status is continuously 
        // updated in the database, maybe we can just read it from the database)
        
        //throw new OperationNotSupportedException("Not implemented yet");
        return -1; // not implemented yet.
    }
}
