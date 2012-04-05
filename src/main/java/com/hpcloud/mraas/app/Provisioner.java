package com.hpcloud.mraas.app;

import com.hpcloud.mraas.domain.Cluster;
import com.hpcloud.mraas.nova.Client;
import com.hpcloud.mraas.persistence.ClusterDAO;
import com.hpcloud.mraas.ssh.SSH;
import com.yammer.dropwizard.logging.Log;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jclouds.openstack.nova.v1_1.domain.Server;


public class Provisioner extends Thread {
    public void provision(Cluster cluster, ClusterDAO store) {
        (new ProvisionerRunnable(cluster, store)).start();
    }

    private class ProvisionerRunnable extends Thread {
        private final Log LOG = Log.forClass(ProvisionerRunnable.class);
        private Cluster cluster;
        private Client client;
        private ClusterDAO store;
        private Map<String, Server> servers = new HashMap<String, Server>();
        private Set<String> serverNames = new HashSet<String>();
        private String privateKey;
        private String BASE_IMAGE_NAME = "Ubuntu Lucid 10.04 LTS Server 64-bit 20111212";
        private String HADOOP_IMAGE_NAME = "Hadoop";

        public ProvisionerRunnable(Cluster cluster, ClusterDAO store) {
            this.cluster = cluster;
            this.store = store;
            this.client = new Client(cluster.getTenant(), cluster.getAccessKey(), cluster.getSecretKey());
            if (cluster.getNodeIds() == null) {
                cluster.setNodeIds(new HashMap<String, String>());
            }
        }

        public void run() {
            setupKeyPair();
            servers = createImage(HADOOP_IMAGE_NAME);
            servers = provisionNodes();
//            sleep(60);
            servers = assignPublicIps();
            sleep(10);
            setupSSHKeys();
            updateEtcHosts();
            configureHadoop();
            startHadoop();
            LOG.info("Finished provisioning cluster {}", cluster.getId());
        }


        private void sleep(int seconds) {
            try {
                Thread.sleep(1000 * seconds);
            } catch (Exception e) { }
        }

        private void setupKeyPair() {
            privateKey = client.newPrivateKey("hadoop");
            LOG.debug("Created private key: {}", privateKey);
        }

        private Server createHost(String name, String imageName) {
            LOG.debug("Creating host {} for cluster {}", name, cluster.getId());
            Server s = client.createHost(name, "hadoop", imageName);
            serverNames.add(name);
            return s;
        }

        public Map<String, Server>  provisionNodes() {
            createHost("master", HADOOP_IMAGE_NAME);
            for (Integer i = 0; i < cluster.getNumNodes(); i++) {
                createHost("hadoop" + i.toString(), HADOOP_IMAGE_NAME);
            }
            servers = client.refreshServers(serverNames);
            for (Server s : servers.values()) {
                client.waitForServer(s.getId());
            }
            return client.refreshServers(serverNames);
        }

        private Map<String, Server> assignPublicIps() {
            for (Server s : servers.values()) {
                LOG.debug("Assigning public ips to {} for cluster {}", s.getName(), cluster.getId());
                client.assignPublicIP(s);
            }
            return client.refreshServers(serverNames);
        }

        private Map<String, Server>  createImage(String imageName) {
            if (client.imageExists(imageName)) return servers;
            LOG.info("Building hadoop base image");
            Server s = createHost("image", BASE_IMAGE_NAME);
            client.waitForServer(s.getId());
            client.assignPublicIP(s);
            sleep(30);
            servers = client.refreshServers(serverNames);
            installHadoop(servers.get("image"));
            client.createImage(imageName, servers.get("image"));
            client.waitForImage(imageName);
            client.destroyHost(servers.get("image"));
            serverNames.remove("image");
            return client.refreshServers(serverNames);
        }

        private void installHadoop(Server s) {
            String ip = client.publicIP(s);

            //dramatically speed up ssh login
            SSH.ssh_cmd(ip, privateKey, "echo \"UseDNS no\" >> /etc/ssh/sshd_config && service ssh restart");


            SSH.ssh_cmd(ip, privateKey, "addgroup hadoop && " +
                "adduser --system --shell /bin/bash --ingroup hadoop hadoop &&" +
                "echo \"hadoop ALL=(ALL) NOPASSWD:ALL\" >> /etc/sudoers");

            // sun java 6
            SSH.ssh_cmd(ip, privateKey, "add-apt-repository ppa:ferramroberto/java && " +
                "apt-get update && " +
                "echo \"sun-java6-jdk shared/accepted-sun-dlj-v1-1 select true\" | debconf-set-selections && " +
                "echo \"sun-java6-jre shared/accepted-sun-dlj-v1-1 select true\" | debconf-set-selections && " +
                "DEBIAN_FRONTEND=noninteractive aptitude install -y -f sun-java6-jre sun-java6-bin sun-java6-jdk && " +
                "update-java-alternatives -s java-6-sun");

            // hadoop distribution
            SSH.ssh_cmd(ip, privateKey, "cd /usr/local && " +
                "wget http://apache.mesi.com.ar//hadoop/common/hadoop-0.20.203.0/hadoop-0.20.203.0rc1.tar.gz && " +
                "tar zxf hadoop-0.20.203.0rc1.tar.gz && " +
                "mv hadoop-0.20.203.0 hadoop && " +
                "chown -R hadoop:hadoop hadoop");

            // hadoop dirs
            SSH.ssh_cmd(ip, privateKey, "cd /usr/local/hadoop/conf && chown hadoop:hadoop ./*");
            SSH.ssh_cmd(ip, privateKey, "mkdir -p /usr/local/hadoop/logs && chown -R hadoop:hadoop /usr/local/hadoop/logs");
            SSH.ssh_cmd(ip, privateKey, "mkdir -p /usr/local/tmp/hadoop && chown -R hadoop:hadoop /usr/local/tmp/hadoop");

            // environment
            SSH.ssh_cmd(ip, privateKey, "echo -e \"export HADOOP_HOME=/usr/local/hadoop\nexport JAVA_HOME=/usr/lib/jvm/java-6-sun\nexport PATH=\\$PATH:\\$HADOOP_HOME/bin\" >> /home/hadoop/.bashrc && chown hadoop:hadoop /home/hadoop/.bashrc");
            SSH.ssh_cmd(ip, privateKey, "echo -e \"export JAVA_HOME=/usr/lib/jvm/java-6-sun\nexport HADOOP_OPTS=-Djava.net.preferIPv4Stack=true\" >> /usr/local/hadoop/conf/hadoop-env.sh");
            SSH.ssh_cmd(ip, privateKey, "mkdir /home/hadoop/.ssh && chown hadoop:hadoop /home/hadoop/.ssh && " +
                "echo -e \"UserKnownHostsFile /dev/null\nStrictHostKeyChecking no\" >> /home/hadoop/.ssh/config && " +
                "chown hadoop:hadoop /home/hadoop/.ssh/config");
        }

        private void configureHadoop() {
            String masterIP = client.publicIP(servers.get("master"));
            String hadoopConfDir = "/usr/local/hadoop/conf/";
            String coreConfig = coreSiteConfig(masterIP).replace("\n", "\\n");
            String hdfsConfig = HDFSSiteConfig().replace("\n", "\\n");
            String mapRedConfig = mapRedSiteConfig(masterIP).replace("\n", "\\n");

            String slaveIPs = "";
            for (Server s : servers.values()) {
                if (s.getName().matches("^hadoop\\d+")) {
                    slaveIPs = slaveIPs + client.privateIP(s) + "\\n";
                }
            }

            for (Server s : servers.values()) {
                SSH.ssh_cmd(client.publicIP(s), privateKey, "echo -e \"" + coreConfig + "\\n\" > " + hadoopConfDir + "core-site.xml");
                SSH.ssh_cmd(client.publicIP(s), privateKey, "echo -e \"" + hdfsConfig + "\\n\" > " + hadoopConfDir + "hdfs-site.xml");
                SSH.ssh_cmd(client.publicIP(s), privateKey, "echo -e \"" + mapRedConfig + "\\n\" > " + hadoopConfDir + "mapred-site.xml");
                SSH.ssh_cmd(client.publicIP(s), privateKey, "echo -e \"" + client.privateIP(servers.get("master")) + "\\n\" > " + hadoopConfDir + "masters");
                SSH.ssh_cmd(client.publicIP(s), privateKey, "echo -e \"" + slaveIPs + "\\n\" > " + hadoopConfDir + "slaves");
            }
        }

        private void startHadoop() {
            String master_ip = client.publicIP(servers.get("master"));

            SSH.ssh_cmd(master_ip, privateKey, "sudo -u hadoop /usr/local/hadoop/bin/hadoop namenode -format");
            SSH.ssh_cmd(master_ip, privateKey, "sudo -u hadoop /usr/local/hadoop/bin/start-all.sh");
        }

        private void setupSSHKeys() {
        	String master_ip = client.publicIP(servers.get("master"));

            SSH.ssh_cmd(master_ip, privateKey, "sudo -u hadoop ssh-keygen -t rsa -N '' -f /home/hadoop/.ssh/id_rsa");

            String keyFile = SSH.getRemoteFile(master_ip, privateKey, "/home/hadoop/.ssh/id_rsa.pub");

            for (Server s : servers.values()) {
                String ip = client.publicIP(s);
                SSH.ssh_cmd(ip, privateKey, "mkdir /home/hadoop/.ssh && chown -R hadoop:hadoop /home/hadoop/.ssh");
                SSH.ssh_cmd(ip, privateKey, "echo \"" + keyFile + "\" >> /home/hadoop/.ssh/authorized_keys");
                SSH.ssh_cmd(ip, privateKey, "chown hadoop:hadoop /home/hadoop/.ssh/authorized_keys && chmod 600 /home/hadoop/.ssh/authorized_keys");
            }
        }

        private void updateEtcHosts() {
            String update_cmd = "echo -e \"";
            for (Server s : servers.values()) {
                String private_ip = client.privateIP(s);
                update_cmd = update_cmd + client.privateIP(s) + " " + s.getName() + ".novalocal" + "\\n";
            }
            update_cmd = update_cmd + "\" >> /etc/hosts";
            for (Server s : servers.values()) {
                SSH.ssh_cmd(client.publicIP(s), privateKey, update_cmd);
            }
        }


        private String coreSiteConfig(String masterIP) {
            return ""
+ "<configuration>\n"
+ "  <property>\n"
+ "    <name>fs.default.name</name>\n"
+ "    <value>hdfs://" + masterIP + ":9000</value>\n"
+ "  </property>\n"
+ "  <property>\n"
+ "    <name>hadoop.tmp.dir</name>\n"
+ "    <value>/usr/local/tmp/hadoop</value>\n"
+ "  </property>\n"
+ "</configuration>\n";
        }

        private String mapRedSiteConfig(String masterIP) {
            return ""
+ "<configuration>\n"
+ "  <property>\n"
+ "    <name>mapred.job.tracker</name>\n"
+ "    <value>" + masterIP + ":9001</value>\n"
+ "  </property>\n"
+ "  <property>\n"
+ "    <name>mapred.reduce.tasks</name>\n"
+ "    <value>15</value>\n"
+ "  </property>\n"
+ "  <property>\n"
+ "    <name>mapred.tasktracker.map.tasks.maximum</name>\n"
+ "    <value>3</value>\n"
+ "  </property>\n"
+ "  <property>\n"
+ "    <name>mapred.tasktracker.reduce.tasks.maximum</name>\n"
+ "    <value>3</value>\n"
+ "  </property>\n"
+ "</configuration>\n";

        }

        private String HDFSSiteConfig() {
            return ""
+ "<configuration>\n"
+ "  <property>\n"
+ "    <name>dfs.replication</name>\n"
+ "    <value>3</value>\n"
+ "  </property>\n"
+ "</configuration>";
        }
    }
}
