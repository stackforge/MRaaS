package com.hpcloud.mraas.nova;

import com.yammer.dropwizard.logging.Log;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.openstack.nova.NovaAsyncClient;
import org.jclouds.openstack.nova.v1_1.NovaClient;
import org.jclouds.openstack.nova.v1_1.domain.Address;
import org.jclouds.openstack.nova.v1_1.domain.Flavor;
import org.jclouds.openstack.nova.v1_1.domain.FloatingIP;
import org.jclouds.openstack.nova.v1_1.domain.Image;
import org.jclouds.openstack.nova.v1_1.domain.KeyPair;
import org.jclouds.openstack.nova.v1_1.domain.Server;
import org.jclouds.openstack.nova.v1_1.extensions.FloatingIPClient;
import org.jclouds.openstack.nova.v1_1.extensions.KeyPairClient;
import org.jclouds.openstack.nova.v1_1.features.FlavorClient;
import org.jclouds.openstack.nova.v1_1.features.ImageClient;
import org.jclouds.openstack.nova.v1_1.features.ServerClient;
import org.jclouds.openstack.nova.v1_1.options.CreateServerOptions;
import org.jclouds.rest.RestContext;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.Module;

public class Client {
    private static final Log LOG = Log.forClass(Client.class);
    private ComputeService cs;
    private NovaClient nova;
    private String ZONE_ID = "az-1.region-a.geo-1";
    private String FLAVOR_NAME = "standard.small";
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
    }


    public Server createHost(String name, String keyPairName, String imageName) {
        LOG.debug("creating server {}", name);
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        Server s = serverClient.createServer(name, imageIdByName(imageName), flavorIdByName(FLAVOR_NAME), (new CreateServerOptions()).keyPairName(keyPairName));
        servers.add(s);
        return s;
    }

    public void destroyHost(Server server) {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        serverClient.deleteServer(server.getId());
    }

    public String publicIP(Server s) {
        Multimap<Address.Type, Address> addresses = s.getAddresses();
        return addresses.get(Address.Type.PUBLIC).iterator().next().getAddr();
    }

    public String privateIP(Server s) {
        Multimap<Address.Type, Address> addresses = s.getAddresses();
        return addresses.get(Address.Type.PRIVATE).iterator().next().getAddr();
    }

    public Boolean imageExists(String name) {
        return (imageIdByName(name) != null);
    }

    // returns imageId.
    public String createImage(String name, Server server) {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        return serverClient.createImageFromServer(name, server.getId());
    }



    public void waitForImage(String imageName) {
        Image i;
        do {
            i = imageByName(imageName);
            if (i.getStatus().equals(Image.Status.ERROR)) return; //TODO throw new Exception("error creating image");
            LOG.debug("Waiting for image {}", imageName);
            try { Thread.sleep(5000); } catch (Exception e) {}
        } while (!i.getStatus().equals(Image.Status.ACTIVE));
    }

    public String imageIdByName(String name) {
        Image i = imageByName(name);
        return ((i == null) ? null : i.getId());
    }

    private Image imageByName(String name) {
        ImageClient imageClient = this.nova.getImageClientForZone(ZONE_ID);
        for (Image i : imageClient.listImagesInDetail()) {
            if (i.getName().equals(name)) return i;
        }
        return null;
    }

    public void waitForServer(String serverID) {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        Server s;
        do {
            s = serverClient.getServer(serverID);
            if (s.getStatus().equals(Server.Status.ERROR)) return; // TODO
            LOG.debug("Waiting for server {}", s.getName());
            try { Thread.sleep(5000); } catch (Exception e) {}
        } while (!s.getStatus().equals(Server.Status.ACTIVE));
    }

    public Map<String, Server> refreshServers(Set<String> names) {
        ServerClient serverClient = this.nova.getServerClientForZone(ZONE_ID);
        Map<String, Server> res = new HashMap<String, Server>();
        for (Server s : serverClient.listServersInDetail()) {
            if (names.contains(s.getName())) {
                res.put(s.getName(), s);
            }
        }
        return res;
    }

    public String flavorIdByName(String name) {
        FlavorClient flavorClient = this.nova.getFlavorClientForZone(ZONE_ID);
        for (Flavor f : flavorClient.listFlavorsInDetail()) {
            if (f.getName().equals(name)) return f.getId();
        }
        return null;
    }

    public String newPrivateKey(String name) {
        KeyPairClient kpClient = nova.getKeyPairExtensionForZone(ZONE_ID).get();
        KeyPair keyPair = kpClient.createKeyPair(name);
        return keyPair.getPrivateKey();
    }

    private FloatingIP getFreeIP() {
        FloatingIPClient fipClient = nova.getFloatingIPExtensionForZone(ZONE_ID).get();
        for (FloatingIP fip : fipClient.listFloatingIPs()) {
            if (fip.getInstanceId() == null) return fip;
        }
        return fipClient.allocate();
    }

    public void assignPublicIP(Server s) {
        FloatingIPClient fipClient = nova.getFloatingIPExtensionForZone(ZONE_ID).get();
        fipClient.addFloatingIPToServer(getFreeIP().getIp(), s.getId());
    } 
}
