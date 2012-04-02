package com.hpcloud.mraas.core;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceJob implements Runnable {
    public static final File TMP_DIR_PARENT = new File("/tmp");
    
    public static final String CONFIG_CORE_SITE = "core-site.xml";
    public static final String CONFIG_MAPRED_SITE = "mapred-site.xml";
    public static final String CONFIG_HDFS_SITE = "hdfs-site.xml";
    
    private static final Logger logger = LoggerFactory.getLogger(MapReduceJob.class);
    
    private static long counter = 1;  
    
    private Long id;
    private MapReduceJobState state;
    private MapReduceJobError error;
    
    private Map<String, String> configs;
    private File jarFile;
    private String[] jobArgs;
    private Cluster cluster;
    
    private File tmpDir;

    public MapReduceJob(Map<String, String> configs, File jarFile, String[] jobArgs, Cluster cluster) {
        this.configs = configs;
        this.jarFile = jarFile;
        this.jobArgs = jobArgs;
        this.cluster = cluster;
        
        synchronized (MapReduceJob.class) {
            this.id = counter++;
        }
        this.state = MapReduceJobState.INITIALIZED;
        this.error = null;
    }
    
    public Long id() {
        return this.id;
    }
    
    @Override
    public void run() {
        state = MapReduceJobState.RUNNING;
        try {
            State jtstate = cluster.jobTrackerState();
            if (jtstate != State.RUNNING) {
                error = new MapReduceJobError("job traking is not in running state: " + jtstate, null);
                state = MapReduceJobState.ERROR;
                return;
            }
            
            tmpDir = createTmpDir(this.cluster);
            Map<String, File> configFiles = createConfigFiles(configs);
            File[] files = configFiles.values().toArray(new File[0]);
            JobRunner jobRunner = new JobRunner(jarFile, jobArgs, files, tmpDir);
            jobRunner.runJar();
            state = MapReduceJobState.RETURNED;
            logger.debug("Custom jar file execution completed [id: {}].", id);
            
            // check if there are any jobs to complete in the cluster
            JobStatus[] jobs = cluster.jobsToComplete();
            if (jobs == null || jobs.length == 0) {
                state = MapReduceJobState.COMPLETED;
                logger.debug("there is no jobs to be completed in the cluster [id: {}].", id);
            } else {
                state = MapReduceJobState.CLUSTER_HAS_JOBS;
                logger.debug("job(s) to complete in cluster: {} [id: {}]", jobs.length, id);
            }
        } catch (Throwable e) {
            state = MapReduceJobState.EXCEPTION;
            error = new MapReduceJobError(e.getMessage(), e);
            logger.error("Exception in running map-reduce job: {} ", this.toString());
            e.printStackTrace();
        }
    }
    
    public MapReduceJobError error() {
        return error;
    }
    
    public MapReduceJobState state() {
        return state;
    }
        
    private File createTmpDir(Cluster cluster) throws IOException {
        String tmpDirName = cluster.nameNodeIP + System.currentTimeMillis();
        File dir = new File(TMP_DIR_PARENT, tmpDirName);
        if (dir.exists()) {
            // hmm, with same milliseconds?
            throw new IOException("tmp dir already exists: " + dir + " [id: " + id + "]");
        }
        if (dir.mkdirs()) {
            logger.debug("tmp dir created: {} [id: {}]", dir, id);
        } else {
            throw new IOException("tmp dir creation failed: " + dir + "[id: " + id + "]");
        }
        return dir;
    }
    
    private Map<String, File> createConfigFiles(Map<String, String> configs) throws IOException {
        Map<String, File> configFiles = new HashMap<String, File>();
        
        // assuming that the keys are the config file names
        for (String fileName : configs.keySet()) {
            String configXML = configs.get(fileName);
            File configFile = new File(tmpDir, fileName);
            FileWriter writer = new FileWriter(configFile);
            writer.write(configXML);
            writer.close();
            configFiles.put(fileName, configFile);
        }
                
        return configFiles;
    }
    
    @Override
    public String toString() {
        return "id: " + id + ", job jar file: " + jarFile + ", cluster: [" + cluster + "]";
    }
    
//    // TODO: needs to be removed, for testing purpose here.
//    public static void main(String[] args) throws Exception {
//        Map<String, String> configs = new HashMap<String, String>();
//        configs.put("core-site.xml", readFile("core-site.xml"));
//        configs.put("mapred-site.xml", readFile("mapred-site.xml"));
//        
//        File jarFile = new File("friendrecommender.jar");
//        if (!jarFile.exists()) {
//            logger.error("jar file does not exist: {}", jarFile);
//            return;
//        }
//        String[] jobArgs = { "dataset", "dataout" };
//        
//        Cluster cluster = new Cluster("10.4.15.153", 8020, 8021);
//
//        MapReduceJob mrjob = new MapReduceJob(configs, jarFile, jobArgs, cluster);
//        Thread t = new Thread(mrjob);
//        t.start();
//        
//        try {
//            while (true) {
//                JobStatus[] clusterJobs = cluster.jobsToComplete();
//                if (clusterJobs == null || clusterJobs.length == 0) {
//                    if (mrjob.state() == MapReduceJobState.INITIALIZED 
//                            || mrjob.state() == MapReduceJobState.RUNNING) {
//                        // it's possible that jobs are not submitted yet, wait a sec
//                        Thread.sleep(1000);
//                    } else {
//                        logger.info("no job in cluster");
//                        break;
//                    }
//                }
//                logger.info("# of jobs to be completed: " + clusterJobs.length);
//                for (JobStatus js : clusterJobs) {
//                    Object[] jobInfo = { js.getJobID(), JobStatus.getJobRunState(js.getRunState()), new Date(js.getStartTime()), js.mapProgress(), js.reduceProgress() };
//                    logger.info("job id: {}, job status: {}, job start time: {}, map progress: {}, reduce progress: {}", jobInfo);
//                }
//                
//                // wait a few seconds before polling again
//                Thread.sleep(1000*10);
//            }
//        } catch (Throwable e) {
//            e.printStackTrace();
//            if (mrjob.error() != null) {
//                logger.error(mrjob.error().errorMessage());
//            }
//        }
//        
//        t.join();
//    }
    
    // for test purposes only
    public static String readFile(String fileName) throws IOException {
        File file = new File(fileName);
        FileReader reader = new FileReader(file);
        StringBuilder content = new StringBuilder();
        char[] buf = new char[1024];
        int c;
        while ((c = reader.read(buf)) != -1) {
            content.append(buf, 0, c);
        }
        
        return content.toString();
    }

    // TODO: temporary class, to be replaced by mraas cluster class
    public static class Cluster {
        protected String nameNodeIP;
        protected Integer hdfsPort;
        protected Integer jobTrackerPort;

        public Cluster(String nameNodeIP, Integer hdfsPort, Integer jobTrackerPort) {
            this.nameNodeIP = nameNodeIP;
            this.hdfsPort = hdfsPort;
            this.jobTrackerPort = jobTrackerPort;
        }

        protected Configuration configuration() {
            String dfsName = "hdfs:///" + String.valueOf(nameNodeIP) + ":" + hdfsPort;
            String jobtracker = nameNodeIP + ":" + jobTrackerPort;
            
            Configuration conf = new Configuration();
            conf.set("fs.default.name", dfsName);
            conf.set("mapreduce.jobtracker.address", jobtracker);
            conf.set("mapreduce.framework.name", "classic");
            
            return conf;
        }
        
        protected JobClient jobTrackerClient() throws Throwable {
            InetSocketAddress sa = new InetSocketAddress(InetAddress.getByName(nameNodeIP), jobTrackerPort);
            JobClient jc = new JobClient(sa, configuration());
            return jc;
        }
        
        protected State jobTrackerState() throws Throwable {
            JobClient jc = jobTrackerClient();
            return jc.getClusterStatus().getJobTrackerState();
        }
        
        protected JobStatus[] jobsToComplete() throws Throwable {
            JobClient jc = jobTrackerClient();
            JobStatus[] js = jc.jobsToComplete();
            return js;
        }
        
        public String toString() {
            return "master node: " + nameNodeIP + ", hdfs port: " + hdfsPort + ", jt port: " + jobTrackerPort;
        }
    }

}














