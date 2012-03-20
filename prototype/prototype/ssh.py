import Queue
import subprocess
import threading
import paramiko
from common import logger

# paramiko.SSHClient.connect is not thread-safe.
paramiko_lock = threading.Lock()


def run_cmd(host, keyfile, cmd, expect_status=0):
    """
        run cmd on host via ssh using keyfile.
        if the result of the command is different
        than expect_status, raise an exception.
    """
    logger.debug("Running \"{0}\" on {1}".format(cmd, host))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy()) # ignore unknown hosts
    paramiko_lock.acquire()
    ssh.connect(host, username='root', key_filename=keyfile, password='')
    paramiko_lock.release()
    chan = ssh.get_transport().open_session()
    chan.exec_command(cmd)
    stdin = chan.makefile('wb')
    stdout = chan.makefile('rb')
    stderr = chan.makefile_stderr('rb')
    status = chan.recv_exit_status()
    if status != expect_status:
        raise Exception("command failed ({0}) on host {1}: {2}\n{3}".format(status, host, cmd, ''.join(stdout.readlines() + stderr.readlines())))
    ssh.close()


def scp(keyfile, src, dest, recursive=False):
    cmd = "scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i {0} ".format(keyfile)
    if recursive:
        cmd = cmd + " -r "
    cmd = cmd + src + ' ' + dest
    logger.debug("Running \"{0}\"".format(cmd))
    o = open('/dev/null', 'w')
    i = open('/dev/null', 'r')
    subprocess.call(cmd, shell=True, stdout=o, stderr=o, stdin=i)
    o.close()
    i.close()


def run_on_hosts(hosts, keyfile, command, expect_status=0):
    """
        run 'command' on all 'hosts' in parallel.
    """
    threads = []
    exceptions = Queue.Queue()
    for host in hosts:
        t = Runner(host, keyfile, command, expect_status, exceptions)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    if not exceptions.empty():
        raise exceptions.get()


class Runner(threading.Thread):
    def __init__(self, host, keyfile, command, expect_status, exceptions):
        threading.Thread.__init__(self)
        self.host = host
        self.keyfile = keyfile
        self.command = command
        self.expect_status = expect_status
        self.exceptions = exceptions
    def run(self):
        try:
            run_cmd(self.host, self.keyfile, self.command, self.expect_status)
        except Exception as e:
            self.exceptions.put(e)
