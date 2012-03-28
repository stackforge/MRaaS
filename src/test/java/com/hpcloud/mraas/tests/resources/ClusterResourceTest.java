package com.hpcloud.mraas.tests.resources;

import com.yammer.dropwizard.testing.ResourceTest;

import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.persistence.ClusterDAO;
import com.hpcloud.mraas.app.Provisioner;
import com.hpcloud.mraas.app.Destroyer;
import com.hpcloud.mraas.resources.ClusterResource;
import com.hpcloud.mraas.resources.ClustersResource;

import org.junit.Before;
import org.junit.Test;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;
import org.mockito.ArgumentMatcher;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ClusterResourceTest extends ResourceTest {
    static {
        Logger.getLogger("com.sun.jersey").setLevel(Level.OFF);
    }

    private final Cluster cluster = new Cluster(new Long(1), 20);
    private final ClusterDAO store = mock(ClusterDAO.class);
    private final Provisioner provisioner = mock(Provisioner.class);
    private final Destroyer destroyer = mock(Destroyer.class);

    @Before
    public void initialize() {
        cluster.setAuthUrl("http://example.com");
        cluster.setAccessKey("123");
        cluster.setSecretKey("456");
        cluster.setTenant("tenant");
    }

    @Override
    protected void setUpResources() {
        when(store.findById(anyLong())).thenReturn(cluster);
        when(store.create(argThat(new IsCluster()))).thenReturn(new Long(1));
        addResource(new ClusterResource(store, destroyer));
        addResource(new ClustersResource(store, provisioner));
    }

    @Test
    public void findByIdTest() throws Exception {
        assertThat("GET requests fetch the cluster by ID",
            client().resource("/cluster/1").get(Cluster.class),
            is(cluster));
        verify(store, times(1)).findById(1);
        verify(store, never()).create(argThat(new IsCluster()));
    }

    @Test
    public void createClusterTest() throws Exception {
        Cluster request = new Cluster();
        request.setNumNodes(20);
        assertThat("POST-ing a new cluster with no id returns the new cluster",
            client().resource("/clusters").accept(MediaType.APPLICATION_JSON_TYPE).post(Cluster.class, request),
            is(cluster));
        verify(store, times(1)).create(argThat(new IsCluster()));
        verify(store, times(1)).findById(1);
        verify(provisioner, times(1)).provision(cluster, store);
    }

    @Test
    public void destroyClusterTest() throws Exception {
        client().resource("/cluster/1").accept(MediaType.APPLICATION_JSON_TYPE).delete();
        verify(store, times(1)).deleteById(new Long(1));
        verify(destroyer, times(1)).destroy(cluster);
    }


}

class IsCluster extends ArgumentMatcher<Cluster> {
    public boolean matches(Object arg) {
        return (arg instanceof Cluster);
    }
}
