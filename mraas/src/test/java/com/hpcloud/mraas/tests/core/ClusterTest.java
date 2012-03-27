package com.hpcloud.mraas.tests.core;

import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Test;
import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.hpcloud.mraas.core.Cluster;


public class ClusterTest {
    private Cluster cluster;

    @Before
    public void initialize() {
        cluster = new Cluster(new Long(1), 20);
        cluster.setAuthUrl("http://foo");
        cluster.setUserName("foo");
        cluster.setPassword("bar");
        cluster.setTenant("thetenant");
    }

    @Test
    public void serializesToJSON() throws Exception {
        assertThat("a cluster can be serialized to JSON",
            asJson(cluster),
            is(equalTo(jsonFixture("fixtures/cluster.json"))));
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        assertThat("a cluster can be deserialized from JSON",
            fromJson(jsonFixture("fixtures/cluster.json"), Cluster.class),
            is(cluster));
    }

}
