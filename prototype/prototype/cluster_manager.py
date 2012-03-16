import os

import novaclient.v1_1

import hadoop_cluster
import config

home = os.getenv('HOME')
state_dir = home + '/.hadoop_clusters/'
if not os.path.exists(state_dir):
    os.mkdir(state_dir)


def new_cluster_name():
    n = 0
    cluster_dir = state_dir + 'cluster' + str(n) + '/'
    while os.path.exists(cluster_dir):
        n = n + 1
        cluster_dir = state_dir + 'cluster' + str(n) + '/'
    return 'cluster' + str(n)


def nova_client():
    return novaclient.v1_1.client.Client(config.name, config.password, config.project_id, config.auth_url, service_type='compute')

def create_cluster(options):
    if not options.name:
        options.name = new_cluster_name()
    cluster_dir = state_dir + options.name + '/'
    os.mkdir(cluster_dir)
    cluster = hadoop_cluster.HadoopCluster(nova_client(), options.name, cluster_dir)
    cluster.provision(config.num_data_nodes, config.flavor_name, config.base_image_name, config.hadoop_image_name)
    cluster.to_file()
    return cluster

def demo(options):
    cluster = create_cluster(options)
    cluster.run_map_reduce_job()

def run_job(options):
    d = state_dir + options.name
    cluster = hadoop_cluster.HadoopCluster(nova_client(), options.name, d)
    cluster.from_file()
    cluster.run_map_reduce_job()
        

def list_clusters():
    for dname in sorted(os.listdir(state_dir)):
        d = state_dir + dname
        cluster = hadoop_cluster.HadoopCluster(nova_client(), dname, d)
        cluster.from_file()
        cluster.print_info()

def destroy_cluster(name):
    cluster_dir = state_dir + name + '/'
    if not os.path.exists(cluster_dir):
        raise Exception('unknown cluster : ' + name)
    cluster = hadoop_cluster.HadoopCluster(nova_client(), name, cluster_dir)
    cluster.from_file()
    cluster.destroy()
