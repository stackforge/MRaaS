import re
import os
import sys
import time
import novaclient.v1_1 as nc

def log(msg):
    print msg

class ClusterProvision(object):

    def __init__(self, username, password, project_id, auth_url):
        self.nova = nc.client.Client(username, password, project_id, auth_url, service_type='compute')
        usuffix = username.replace('.','_').replace('@','_')
        self.security_group_name = 'sg_hadoop_%s_%d' % (usuffix, int(time.time()))
        self.servers = {}

    def cleanup(self):
        os.remove(self.private_key_filename)
        for server in self.servers.itervalues():
            self.destroy_vm(server.id)
        self.keypair.delete()

    def create_security_group(self):
        sgm = nc.security_groups.SecurityGroupManager(self.nova)
        sg = sgm.create(self.security_group_name, 'temporary security group created for hadoop cluster')
        self.security_group_id = sg.id
        return sg

    def add_security_group_rule(self, security_group_id):
        sgrm = nc.security_group_rules.SecurityGroupRuleManager(self.nova)
        srule = sgrm.create(security_group_id, 'tcp', 1, 65535, '0.0.0.0/0')
        self.srule_id = srule.id
        return srule

    def create_key_pair(self):
        kpmgr = nc.keypairs.KeypairManager(self.nova)
        kname = 'mraas_generated_key_%d' % int(time.time())
        kp = kpmgr.create(kname)
        self.keypair = kp
        return kp

    def save_private_key(self, keypair):
        filename = '%s.pem' % keypair.uuid
        f = open(filename, 'w')
        f.write(keypair.private_key)
        f.close()
        os.chmod(filename, 0600)
        self.private_key_filename = filename
        log("Saved private key as {0}".format(filename))
        return filename

    def create_vm(self, hostname, security_groups, keypair_name, flavor_id=103, image_id=1242):
        flavor = self.nova.flavors.get(flavor_id)
        image = self.nova.images.get(image_id)
        vm = client.servers.create(hostname, security_groups, image, flavor, key_name=keypair_name)
        return vm

    def destroy_vm(self, vm_id):
        log("Destroying vm id {0}".format(vm_id))
        self.nova.servers.delete(vm_id)

    def assign_public_ip(self, vm_id):
        ip = None
        fl = self.nova.floating_ips.list()
        for flip in fl:
            if flip.instance_id is None:
                # Choose one of the unassigned IPs
                ip = flip.ip
                break
        if ip is None:
            floating_ip = self.nova.floating_ips.create(None)
            ip = floating_ip.ip
        # Fail after 30 attempts
        success = False        
        for i in range(30):
            try:
                log('assigning public IP: %s, attempt %d' % (ip, i))
                self.nova.servers.add_floating_ip(vm_id, ip)
                success = True
                break
            except Exception:
                sucess = False
                time.sleep(1)
        if success is False:
            raise exception.InstanceFault()
        return ip


