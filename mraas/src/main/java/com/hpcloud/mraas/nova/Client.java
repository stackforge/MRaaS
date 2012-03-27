package com.hpcloud.mraas.nova;

import org.openstack.client.common.*;
import org.openstack.client.compute.AsyncServerOperation;
import org.openstack.model.compute.NovaFlavor;
import org.openstack.model.compute.NovaImage;
import org.openstack.model.compute.NovaServerForCreate;

public class Client {
    private JerseyOpenstackSession session;


    public Client(String auth_url, String username, String password, String tenant) {
        this.session = new JerseyOpenstackSession();
        OpenstackCredentials creds = new OpenstackCredentials(auth_url, username, password, tenant);
        session.authenticate(creds);
    }

    public AsyncServerOperation createHost(String name, String flavorName, String imageName) {
        NovaServerForCreate request = new NovaServerForCreate();
        request.setName(name);
        request.setFlavorRef(flavorByName(flavorName));
        request.setImageRef(imageByName(imageName));
        return session.getComputeClient().createServer(request);
    }

    public String imageByName(String name) {
        for (NovaImage i : session.getComputeClient().root().images().list()) {
            if (i.getName().equals(name)) return i.getId();
        }
        return null;
    }

    public String flavorByName(String name) {
        for (NovaFlavor f : session.getComputeClient().root().flavors().list()) {
            if (f.getName().equals(name)) return f.getId();
        }
        return null;
    }
}
