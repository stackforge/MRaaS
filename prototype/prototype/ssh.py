from subprocess import call
import paramiko
from common import logger
from threading import Thread



# TODO: I was having problems with paramiko randomly hanging sometimes.
def run_cmd_paramiko(host, keyfile, cmd):
    logger.info("Running \"{0}\" on {1}".format(cmd, host))
    ssh = paramiko.SSHClient()
#    ssh.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy()) # ignore unknown hosts. TODO
    ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
    ssh.connect(host, username='root', key_filename=keyfile, password='')
#    stdin, stdout, stderr = ssh.exec_command(cmd)
#    print stdout.readlines()
    chan = ssh.get_transport().open_session()
    chan.exec_command('cmd')
    status = chan.recv_exit_status()
#    if status != 0: raise "command failed on host {0}: {1}".format(host, cmd)
    ssh.close()


def scp(keyfile, src, dest, recursive=False):
    cmd = "scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i {0} ".format(keyfile)
    if recursive:
        cmd = cmd + " -r "
    cmd = cmd + src + ' ' + dest
    logger.info("Running \"{0}\"".format(cmd))
    o = open('/dev/null', 'w')
    i = open('/dev/null', 'r')
    call(cmd, shell=True, stdout=o, stderr=o, stdin=i)
    o.close()
    i.close()


def run_cmd(host, keyfile, cmd):
    logger.info("Running \"{0}\" on {1}".format(cmd, host))
    ssh_opts = "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
    cmd = cmd.replace('"', '\\"')
    o = open('/dev/null', 'w')
    i = open('/dev/null', 'r')
    call("ssh -i {0} {1} root@{2} \"{3}\" 2>&1 > /dev/null".format(keyfile, ssh_opts, host, cmd), shell=True, stdout=o, stderr=o, stdin=i)
    o.close()
    i.close()

# run the same command in parallel on several hosts
def run_on_hosts(hosts, keyfile, command):
    threads = []
    for host in hosts:
        t = Runner(host, keyfile, command)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()


class Runner(Thread):
    def __init__(self, host, keyfile, command):
        Thread.__init__(self)
        self.host = host
        self.keyfile = keyfile
        self.command = command
    def run(self):
        run_cmd(self.host, self.keyfile, self.command)
        self.status = 0 # TODO
