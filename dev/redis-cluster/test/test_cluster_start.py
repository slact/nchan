import time
import unittest
from types import SimpleNamespace
from cluster_start import create_and_start_cluster
from constants import CLUSTER_SHUTDOWN_CMD
from utils import run_subprocess

# declare constants
SLEEP = 10

class TestClusterStart(unittest.TestCase):
    def test_create_cluster_without_auth(self):
        '''Checks if a cluster runs with the same properties as provided by the arguments. 
        This test case checks cluster start without auth.'''
        # define arguments for cluster
        args = SimpleNamespace(shards=3, replicas=1, password=None, auth=False, ip='127.0.0.1', start_port=6500)

    def test_create_and_start_cluster(self):
        '''Checks if a cluster runs with the same properties as provided by the arguments.
        This test case checks cluster start without auth.'''
        # define arguments for cluster
        args = SimpleNamespace(shards=3, replicas=1, password=None, auth=False, ip='127.0.0.1', start_port=7000)

        # run cluster and wait for cluster to start
        cluster = create_and_start_cluster(args)
        time.sleep(SLEEP)

        # get info for the running cluster

        # get info for the running cluster
        cluster_info = cluster.get_cluster_info()
        node_roles = [node.role for node in cluster_info]

        # assert all argument values
        assert args.shards == node_roles.count('master')
        assert args.replicas*args.shards == node_roles.count('slave')
        ip = [node.ip for node in cluster_info]
        assert len(set(ip)) == 1
        assert args.ip == min(ip)
        ports = [int(node.port_number) for node in cluster_info]
        assert args.start_port == min(ports)
        
        # initiate cluster shutdown and wait till shutdown before running any other test case
        run_subprocess([CLUSTER_SHUTDOWN_CMD])
        time.sleep(SLEEP)

    def test_create_and_start_cluster_with_auth(self):
        '''Checks if a cluster runs with the same properties as provided by the arguments. 
        This test case checks cluster start without auth.'''
        # define arguments for cluster
        args = SimpleNamespace(shards=3, replicas=1, password=None, auth=False, ip='127.0.0.1', start_port=7000)
        
        # run cluster and wait for cluster to start
        cluster = create_and_start_cluster(args)
        time.sleep(SLEEP)

        # get info for the running cluster
        # To get cluster info, password is required. 
        # Hence, this checks auth enable/disable and password
        cluster_info = cluster.get_cluster_info()
        node_roles = [node.role for node in cluster_info]

        # assert all argument values
        assert args.shards == node_roles.count('master')
        assert args.replicas*args.shards == node_roles.count('slave')
        ip = [node.ip for node in cluster_info]
        assert len(set(ip)) == 1
        assert args.ip == min(ip)
        ports = [int(node.port_number) for node in cluster_info]
        assert args.start_port == min(ports)
        
        # initiate cluster shutdown and wait till shutdown before running any other test case
        run_subprocess([CLUSTER_SHUTDOWN_CMD])
        time.sleep(SLEEP)

    def test_create_cluster_with_auth_password(self):
        '''Checks if a cluster runs with the same properties as provided by the arguments. 
        This test case checks cluster start with auth enabled where password is entered by user.'''
        # define arguments for cluster
        password = "password123"
        args = SimpleNamespace(shards=3, replicas=1, password=password, auth=True, ip='127.0.0.1', start_port=7000)
        
        # run cluster and wait for cluster to start
        cluster = create_and_start_cluster(args)
        time.sleep(SLEEP)

        # check if cluster password is set as the same password as set by user
        assert (cluster.password) == password

        # get info for the running cluster
        # To get cluster info, password is required. 
        # Hence, this checks auth enable/disable and password
        cluster_info = cluster.get_cluster_info()
        node_roles = [node.role for node in cluster_info]

        # assert all argument values
        assert args.shards == node_roles.count('master')
        assert args.replicas*args.shards == node_roles.count('slave')
        ip = [node.ip for node in cluster_info]
        assert len(set(ip)) == 1
        assert args.ip == min(ip)
        ports = [int(node.port_number) for node in cluster_info]
        assert args.start_port == min(ports)
        
        # initiate cluster shutdown and wait till shutdown before running any other test case
        run_subprocess([CLUSTER_SHUTDOWN_CMD])
        time.sleep(SLEEP)

    def test_create_cluster_with_auth_no_password(self):
        '''Checks if a cluster runs with the same properties as provided by the arguments. 
        This test case checks cluster start with auth enabled where password is not 
        entered by user but generated randomly.'''
        # define arguments for cluster
        args = SimpleNamespace(shards=4, replicas=3, password=None, auth=True, ip='127.0.0.1', start_port=6379)
        
        # run cluster and wait for cluster to start
        cluster = create_and_start_cluster(args)
        time.sleep(SLEEP)

        # check if cluster password is set as a hex(UUID) password
        assert len(cluster.password) == 32

        # get info for the running cluster
        # To get cluster info, password is required. 
        # Hence, this checks auth enable/disable and password
        cluster_info = cluster.get_cluster_info()
        node_roles = [node.role for node in cluster_info]

        # assert all argument values
        assert args.shards == node_roles.count('master')
        assert args.replicas*args.shards == node_roles.count('slave')
        ip = [node.ip for node in cluster_info]
        assert len(set(ip)) == 1
        assert args.ip == min(ip)
        ports = [int(node.port_number) for node in cluster_info]
        assert args.start_port == min(ports)
        
        # initiate cluster shutdown and wait till shutdown before running any other test case
        run_subprocess([CLUSTER_SHUTDOWN_CMD])
        time.sleep(SLEEP)


if __name__ == '__main__':
    unittest.main()
