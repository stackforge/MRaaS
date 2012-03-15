#!/usr/bin/python -u
import json
import os
import re
import shutil
import subprocess
import tempfile
import time

import novaclient.v1_1 as nc

import config
from ssh import run_cmd, run_on_hosts, scp
from common import select, logger
from termcolor import colored

class HadoopCluster:
    """
        This class provisions, configures and destroys Hadoop clusters.

    """
    def __init__(self, nova_client, cluster_name, work_dir):
        self.nova = nova_client
        self.name = cluster_name
        self.work_dir = work_dir

    def provision(self, num_data_nodes, flavor_name, base_image_name, hadoop_image_name):
        """ provision and configure a cluster, and start hadoop """
        try:
            self.hosts = {}
            self.save_private_key(self.create_key_pair())
            self.create_image(base_image_name, hadoop_image_name, flavor_name)
            self.provision_hosts(num_data_nodes, self.flavor(flavor_name), self.image(hadoop_image_name))
            self.wait_for_hosts()
            self.assign_master_public_ip()
            self.log_ssh_commands()
            self.setup_ssh_keys()
            self.update_etc_hosts()
            self.configure_hadoop()
            self.start_hadoop()
            logger.info(colored('The cluster is up!', 'green', attrs=['bold']))
        except Exception as e:
            logger.error(colored('Encountered an error building the cluster.', 'red'))
            self.destroy()
            raise




    def to_file(self):
        hosts = {}
        for h in self.hosts.keys():
            hosts[h] = self.hosts[h].name
        data = {
            'hosts': hosts,
            'keypair': self.keypair.name,
            'private_key_filename': self.private_key_filename,
            'master_ip': self.master_ip
        }
        f = open(self.work_dir + '/state', 'w')
        f.write(json.dumps(data))
        f.close()

    def from_file(self):
        f = open(self.work_dir + '/state', 'r')
        data = json.loads(f.read())
        f.close
        self.hosts = {}
        current_hosts = self.nova.servers.list()
        for hid in data['hosts'].keys():
            self.hosts[hid] = select(current_hosts, lambda x: x.name == data['hosts'][hid])
        kpmgr = nc.keypairs.KeypairManager(self.nova)
        self.keypair = select(kpmgr.list(), lambda x: x.name == data['keypair'])
        self.private_key_filename = data['private_key_filename']
        self.master_ip = data['master_ip']
        

    def destroy(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)
        if self.keypair: self.keypair.delete()
        for host in self.hosts.itervalues():
            self.destroy_host(host)

    def create_image(self, base_image_name, hadoop_image_name, flavor_name):
        if self.image(hadoop_image_name): return
        logger.info("Building a hadoop image based on {0}.".format(base_image_name))
        self.provision_host('image', self.flavor(flavor_name), self.image(base_image_name))
        self.wait_for_hosts()
        image_host = self.hosts['image']
        self.install_hadoop(image_host)
        self.log_ssh_commands()
        self.hosts['image'].create_image(hadoop_image_name)
        self.wait_for_image(hadoop_image_name)
        self.destroy_host(self.hosts['image'])
        del self.hosts['image']

    def destroy_host(self, host):
        logger.info("Destroying host {0} - {1}".format(host.name, host.id))
        self.nova.servers.delete(host.id)

    def flavor(self, flavor_name):
        return select(self.nova.flavors.list(), lambda x: x.name == flavor_name)

    def image(self, image_name):
        return select(self.nova.images.list(), lambda x: x.name == image_name)

    def provision_host(self, server_name, flavor, image):
        logger.info("Provisioning {0}".format(server_name))
        s = self.nova.servers.create(server_name + '.' + self.name, image, flavor, key_name=self.keypair.name)
        self.hosts[server_name] = s

    def provision_hosts(self, num_data_nodes, flavor, image):
        self.provision_host('master', flavor, image)
        for i in range(num_data_nodes):
            self.provision_host("hadoop" + str(i), flavor, image) 

    def wait_for_image(self, image_name):
        logger.info(colored('Saving image', 'yellow'))
        i = select(self.nova.images.list(), lambda x: x.name == image_name)
        if not i: raise Exception('image was not saved')
        while re.match('SAVING', i.status):
            print('.'),
            time.sleep(1)
            i = select(self.nova.images.list(), lambda x: x.name == image_name)
        print ''
        if not re.match('ACTIVE', i.status): raise Exception('failed to create image')


    # wait until all of the hosts are up
    def wait_for_hosts(self):
        logger.info(colored("Waiting for all hosts to come online", 'yellow'))
        for host in self.hosts.itervalues():
            h = select(self.nova.servers.list(), lambda x: x.name == host.name)
            while re.match('BUILD', h.status):
                print('.'),
                time.sleep(1)
                h = select(self.nova.servers.list(), lambda x: x.name == host.name)
            if re.match('ERROR', h.status): raise Exception("failed to create host {0}".format(host.name))
            self.hosts[host.name.split('.')[0]] = h
        # can't always log into hosts as soon as they become 'ACTIVE'. wait a bit longer.
        for i in range(30):
            print('.'),
            time.sleep(1)
        print('')


    def assign_master_public_ip(self):
        self.master_ip = self.assign_public_ip(self.hosts['master'].id)
        logger.info("Assigned master node public ip: " + self.master_ip)

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
                logger.info('assigning public IP: %s, attempt %d' % (ip, i))
                self.nova.servers.add_floating_ip(vm_id, ip)
                success = True
                break
            except Exception:
                sucess = False
                time.sleep(1)
        if success is False:
            raise exception.InstanceFault()
        return ip

    def create_key_pair(self):
        kpmgr = nc.keypairs.KeypairManager(self.nova)
        kname = 'mraas_generated_key_%d' % int(time.time())
        kp = kpmgr.create(kname)
        self.keypair = kp
        return kp

    def save_private_key(self, keypair):
        filename = '{0}{1}.pem'.format(self.work_dir, keypair.uuid)
        f = open(filename, 'w')
        f.write(keypair.private_key)
        f.close()
        os.chmod(filename, 0600)
        self.private_key_filename = filename
        logger.info("Saved private key as {0}".format(filename))
        return filename

    def host_ip(self, host):
        return host.addresses['private'][0]['addr']

    def ssh_string(self, host):
      return "ssh -i {0} root@{1}".format(self.private_key_filename, self.host_ip(host))

    def log_ssh_commands(self):
        for host in self.hosts.itervalues():
            logger.info("{0}: `{1}`".format(colored(host.name, 'green'), self.ssh_string(host)))

    def ssh_cmd(self, host, command, exit_status=0):
        ip = self.host_ip(host)
        run_cmd(ip, self.private_key_filename, command, exit_status)

    def ssh_cmd_parallel(self, command, exit_status=0):
        run_on_hosts(self.host_ips(), self.private_key_filename, command, exit_status)

    def host_ips(self):
        ips = []
        for host in self.hosts.itervalues():
            ips.append(self.host_ip(host))
        return ips

    def host_ip_by_name(self, name):
        if not self.hosts[name]:
            return None
        return self.host_ip(self.hosts[name])

    def install_hadoop(self, host):
        logger.info(colored('Installing Hadoop', 'yellow'))

        # dramatically speed up ssh login
        self.ssh_cmd(host, 'echo "UseDNS no" >> /etc/ssh/sshd_config && service ssh restart')

        self.ssh_cmd(host, "addgroup hadoop && " +
        "adduser --system --shell /bin/bash --ingroup hadoop hadoop &&" +
        "echo 'hadoop ALL=(ALL) ALL' >> /etc/sudoers")

        # sun java 6
        self.ssh_cmd(host, "add-apt-repository ppa:ferramroberto/java && " +
        "apt-get update && " +
        "echo \"sun-java6-jdk shared/accepted-sun-dlj-v1-1 select true\" | debconf-set-selections && " +
        "echo \"sun-java6-jre shared/accepted-sun-dlj-v1-1 select true\" | debconf-set-selections && " +
        "DEBIAN_FRONTEND=noninteractive aptitude install -y -f sun-java6-jre sun-java6-bin sun-java6-jdk && " +
        "update-java-alternatives -s java-6-sun", 2)

        # hadoop distribution
        self.ssh_cmd(host, "cd /usr/local && " +
        "wget http://apache.mesi.com.ar//hadoop/common/hadoop-0.20.203.0/hadoop-0.20.203.0rc1.tar.gz && " +
        "tar zxf hadoop-0.20.203.0rc1.tar.gz && " +
        "mv hadoop-0.20.203.0 hadoop && " +
        "chown -R hadoop:hadoop hadoop")

        # hadoop dirs
        self.ssh_cmd(host, "cd /usr/local/hadoop/conf && chown hadoop:hadoop ./*")
        self.ssh_cmd(host, 'mkdir -p /usr/local/hadoop/logs && chown -R hadoop:hadoop /usr/local/hadoop/logs')
        self.ssh_cmd(host, 'mkdir -p /usr/local/tmp/hadoop && chown -R hadoop:hadoop /usr/local/tmp/hadoop')

        # environment
        self.ssh_cmd(host, 'echo -e "export HADOOP_HOME=/usr/local/hadoop\nexport JAVA_HOME=/usr/lib/jvm/java-6-sun\nexport PATH=\$PATH:\$HADOOP_HOME/bin" >> /home/hadoop/.bashrc && chown hadoop:hadoop /home/hadoop/.bashrc')
        self.ssh_cmd(host, 'echo -e "export JAVA_HOME=/usr/lib/jvm/java-6-sun\nexport HADOOP_OPTS=-Djava.net.preferIPv4Stack=true" >> /usr/local/hadoop/conf/hadoop-env.sh')
        self.ssh_cmd(host, 'mkdir /home/hadoop/.ssh && chown hadoop:hadoop /home/hadoop/.ssh && '
        'echo -e "UserKnownHostsFile /dev/null\nStrictHostKeyChecking no" >> /home/hadoop/.ssh/config && ' +
        'chown hadoop:hadoop /home/hadoop/.ssh/config')


    def setup_ssh_keys(self):
        logger.info(colored('Setting up SSH keys', 'yellow'))
        self.ssh_cmd(self.hosts['master'], "sudo -u hadoop ssh-keygen -t rsa -N '' -f /home/hadoop/.ssh/id_rsa")

        scp(self.private_key_filename, "root@{0}:/home/hadoop/.ssh".format(self.host_ip_by_name("master")), "{0}/hadoop_keys".format(self.work_dir), recursive=True)
        for host in self.hosts.itervalues():
            scp(self.private_key_filename, "{0}/hadoop_keys/id_rsa.pub".format(self.work_dir), "root@{0}:/home/hadoop/master_key".format(self.host_ip(host)))
        self.ssh_cmd_parallel("if [ ! -d /home/hadoop/.ssh ]; then mkdir /home/hadoop/.ssh; fi && chown hadoop:hadoop /home/hadoop/.ssh && chmod 700 /home/hadoop/.ssh && " +
                "mv /home/hadoop/master_key /home/hadoop/.ssh/authorized_keys && chmod 600 /home/hadoop/.ssh/authorized_keys && chown hadoop:hadoop /home/hadoop/.ssh/authorized_keys")

    def update_etc_hosts(self):
        logger.info(colored('Updating /etc/hosts', 'yellow'))
        host_str = 'echo -e "'
        for host in self.hosts.itervalues():
            host_str = host_str + self.host_ip(host) + ' ' + host.name + ".novalocal\\n"
        host_str = host_str + '" >> /etc/hosts'
        self.ssh_cmd_parallel(host_str)

    def start_hadoop(self):
        self.ssh_cmd(self.hosts['master'], "sudo -u hadoop /usr/local/hadoop/bin/hadoop namenode -format")
        self.ssh_cmd(self.hosts['master'], "sudo -u hadoop /usr/local/hadoop/bin/start-all.sh")

    def configure_hadoop(self):
        logger.info(colored('Configuring Hadoop', 'yellow'))
        core_site = '%s/core-site.xml' % self.work_dir
        self.write_config_file(core_site, self.core_site_config())
        mapred_site = '%s/mapred-site.xml' % self.work_dir
        self.write_config_file(mapred_site, self.mapred_site_config())
        hdfs_site = '%s/hdfs-site.xml' % self.work_dir
        self.write_config_file(hdfs_site, self.hdfs_site_config())

        masters = self.host_ip_by_name('master')
        slaves = ''
        for host in self.hosts.itervalues():
            if re.match('hadoop', host.name):
                slaves = slaves + self.host_ip(host) + "\\n"

        for host in self.hosts.itervalues():
            scp(self.private_key_filename, core_site, "root@{0}:/usr/local/hadoop/conf/.".format(self.host_ip(host)))
            scp(self.private_key_filename, mapred_site, "root@{0}:/usr/local/hadoop/conf/.".format(self.host_ip(host)))
            scp(self.private_key_filename, hdfs_site, "root@{0}:/usr/local/hadoop/conf/.".format(self.host_ip(host)))
        self.ssh_cmd_parallel('echo -e "{0}" > /usr/local/hadoop/conf/masters'.format(masters))
        self.ssh_cmd_parallel('echo -e "{0}" > /usr/local/hadoop/conf/slaves'.format(slaves))
            

    def write_config_file(self, name, contents):
        f = open(name, 'w')
        f.write(contents)
        f.close()


    def core_site_config(self):
        return """
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://{0}:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/usr/local/tmp/hadoop</value>
  </property>
</configuration>
        """.format(self.host_ip_by_name("master"))

    def mapred_site_config(self):
        return """
<configuration>
  <property>
    <name>mapred.job.tracker</name>
    <value>{0}:9001</value>
  </property>
  <property>
    <name>mapred.reduce.tasks</name>
    <value>15</value>
  </property>
  <property>
    <name>mapred.tasktracker.map.tasks.maximum</name>
    <value>3</value>
  </property>
  <property>
    <name>mapred.tasktracker.reduce.tasks.maximum</name>
    <value>3</value>
  </property>
</configuration>
        """.format(self.host_ip_by_name("master"))

    def hdfs_site_config(self):
        return """
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
</configuration>
        """


    def print_info(self):
        print('Cluster: ' + colored(self.name, 'green', attrs=['bold']))
        for h in self.hosts.keys():
            print '        {0}: {1}'.format(colored(h, 'green'), self.ssh_string(self.hosts[h]))
        print ''
