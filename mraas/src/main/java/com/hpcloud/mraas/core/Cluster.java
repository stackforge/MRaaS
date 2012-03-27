package com.hpcloud.mraas.core;

import com.yammer.dropwizard.json.JsonSnakeCase;
import org.codehaus.jackson.annotate.JsonProperty;
import lombok.Data;

@JsonSnakeCase
@Data
public class Cluster {
    @JsonProperty
    private Long id;
    @JsonProperty
    private Integer numNodes;
    @JsonProperty
    private String authUrl;
    @JsonProperty
    private String userName;
    @JsonProperty
    private String password;
    @JsonProperty
    private String tenant;


    public Cluster() {}

    public Cluster(Long id, int numNodes) {
        this.id = id;
        this.numNodes = numNodes;
    }

}
