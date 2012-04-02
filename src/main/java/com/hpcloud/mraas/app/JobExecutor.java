package com.hpcloud.mraas.app;

import java.io.File;
import java.util.Map;

import com.hpcloud.mraas.core.MapReduceJob;
import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.persistence.MapReduceJobDAO;

public class JobExecutor extends Thread {

    public void runJob(Cluster cluster, String jarFile, String[] jobArgs, MapReduceJobDAO store) {
        (new JobExecutorRunner(cluster, jarFile, jobArgs, store)).start();
    }
    
    private class JobExecutorRunner extends Thread {
        private Cluster cluster;
        private String jarFile;
        private String[] jobArgs;
        private Map<String, String> configs;
        private MapReduceJobDAO store;
        
        public JobExecutorRunner(Cluster cluster, 
                                 String jarFile,
                                 String[] jobArgs,
                                 MapReduceJobDAO store) {
            this.cluster = cluster;
            this.jarFile = jarFile;
            this.jobArgs = jobArgs;
            this.store = store;
            
            // TODO: get config files from db via cluster object 
            //this.configs = cluster.configs();
        }



        @Override
        public void run() {
            // TODO: get jar file from swift
            File file = new File(jarFile); // for now assume local path
            
            // TODO: parse jobArgs for input path, and copy it from swift to hdfs
            // for now, assumed that it is already in hdfs
            
            // TODO: need to get IP and port number from Cluster object, and remove
            // MapReduceJob.Cluster class. This is just a mock object for now:
            MapReduceJob.Cluster jc = new MapReduceJob.Cluster("<to be replaced>", 8020, 8021);
            
            MapReduceJob mrjob = new MapReduceJob(configs, file, jobArgs, jc);
            Long mrjobId = mrjob.id();
            
            // TODO: store create?
            
            
            // TODO: already running in a thread, ok to call it directly without creating another thread?
            mrjob.run(); 
            

            // TODO: keep this running until the job completes in the cluster for status update purpose?
        }
    }
   
}
