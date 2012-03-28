package com.hpcloud.mraas.domain;

import com.yammer.dropwizard.json.JsonSnakeCase;
import org.codehaus.jackson.annotate.JsonProperty;
import lombok.Data;
import java.util.Map;
import java.util.HashMap;

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
    private String accessKey;
    @JsonProperty
    private String secretKey;
    @JsonProperty
    private String tenant;

    private Map<String, String> nodeIds;


    public Cluster() {}

    public Cluster(Long id, int numNodes) {
        this.id = id;
        this.numNodes = numNodes;
    }

}
