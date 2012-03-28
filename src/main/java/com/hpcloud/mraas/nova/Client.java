package com.hpcloud.mraas.nova;

import com.yammer.dropwizard.logging.Log;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.openstack.nova.NovaAsyncClient;
import org.jclouds.openstack.nova.v1_1.NovaClient;
import org.jclouds.openstack.nova.v1_1.domain.Flavor;
import org.jclouds.openstack.nova.v1_1.domain.Image;
import org.jclouds.openstack.nova.v1_1.domain.Server;
import org.jclouds.openstack.nova.v1_1.features.FlavorClient;
import org.jclouds.openstack.nova.v1_1.features.ImageClient;
import org.jclouds.openstack.nova.v1_1.features.ServerClient;
import org.jclouds.rest.RestContext;
import lombok.val;

import java.util.Set;
import java.util.HashSet;

public class Client {
    private static final Log LOG = Log.forClass(Client.class);
    private ComputeService cs;
    private NovaClient nova;
    private String ZONE_ID = "az-1.region-a.geo-1";
    private String FLAVOR_NAME = "standard.small";
    private String IMAGE_NAME = "Ubuntu Lucid 10.04 LTS Server 64-bit 20111212";
    private Set<Server> servers;

    public Client(String tenant, String accessKey, String secretKey) {
        val ctx = new ComputeServiceContextFactory().createContext("hpcloud-compute", tenant + ":" + accessKey, secretKey);
        this.cs = ctx.getComputeService();
        RestContext<NovaClient, NovaAsyncClient> context = ctx.getProviderSpecificContext();
        this.nova = context.getApi();
        this.servers = new HashSet<Server>();
        sandbox();
    }



    private void sandbox() {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        for (val s : serverClient.listServersInDetail()) {
            System.out.println(s);
        }
        System.out.println(imageIdByName(IMAGE_NAME)); 
        System.out.println(flavorIdByName(FLAVOR_NAME));
    }


    public void createHosts(Set<String> names) {
        for (String name : names) {
            createHost(name);
        }
    }

    public Server createHost(String name) {
        LOG.debug("creating server {}", name);
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        Server s = serverClient.createServer(name, imageIdByName(IMAGE_NAME), flavorIdByName(FLAVOR_NAME));
        servers.add(s);
        return s;
    }

    public void destroyHost(String id) {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        serverClient.deleteServer(id);
    }

    public String imageIdByName(String name) {
        ImageClient imageClient = this.nova.getImageClientForZone(ZONE_ID);
        for (Image i : imageClient.listImagesInDetail()) {
            if (i.getName().equals(name)) return i.getId();
        }
        return null;
    }

    public String flavorIdByName(String name) {
        FlavorClient flavorClient = this.nova.getFlavorClientForZone(ZONE_ID);
        for (Flavor f : flavorClient.listFlavorsInDetail()) {
            if (f.getName().equals(name)) return f.getId();
        }
        return null;
    }
}
