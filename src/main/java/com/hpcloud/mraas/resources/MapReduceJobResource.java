package com.hpcloud.mraas.resources;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.hpcloud.mraas.app.JobExecutor;
import com.hpcloud.mraas.domain.ClusterJob;
import com.hpcloud.mraas.persistence.MapReduceJobDAO;
import com.yammer.metrics.annotation.Timed;

@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
public class MapReduceJobResource {
    private final MapReduceJobDAO store;
    private final JobExecutor jobExecutor;
    
    public MapReduceJobResource(MapReduceJobDAO store, JobExecutor jobExecutor) {
        this.store = store;
        this.jobExecutor = jobExecutor;
    }


    @POST
    @Timed
    public Long submitJob(ClusterJob job) {
        //final long jobId = store.create(job);
        //jobExecutor.runJob(cluster, jarFile, jobArgs, store)
        return 0L;
    }    
}
