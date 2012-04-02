package com.hpcloud.mraas.domain;

import org.codehaus.jackson.annotate.JsonProperty;

import lombok.Data;

import com.yammer.dropwizard.json.JsonSnakeCase;

@JsonSnakeCase
@Data
public class ClusterJob {

    @JsonProperty
    private Long id; // internal id for a job, not cluster
    
    @JsonProperty
    private String nameNodeIP;
    
    @JsonProperty
    private Integer hdfsPort;
    
    @JsonProperty
    private Integer jobTrackerPort;
    
    @JsonProperty
    private Integer state;

    public ClusterJob(Long id, 
                      String nameNodeIP, 
                      Integer hdfsPort,
                      Integer jobTrackerPort,
                      Integer state) {
        this.id = id;
        this.nameNodeIP = nameNodeIP;
        this.hdfsPort = hdfsPort;
        this.jobTrackerPort = jobTrackerPort;
        this.state = state;
    }

    
}
