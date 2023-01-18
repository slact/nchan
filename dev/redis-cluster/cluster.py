from constants import DEFAULT_IP, CLUSTER_NODES_INFO, REDIS_CLI_CMD
from constants import REDIS_CONFIG_FOLDER, REDIS_START_PORT,CLUSTER_CONFIG_PATH,FILE_MODE_WRITE, \
      REPLICAS_PER_SHARD,USER_AUTH,PASSWORD,NUMBER_OF_SHARDS,EMPTY_STRING,START_PORT
from utils import get_current_path, is_directory_present, remove_directory, create_directory, \
    generate_password,is_file_present,open_file,write_on_file,remove_file,close_file
from cluster_node import ClusterNode
from utils import run_subprocess
from subprocess import PIPE
import json
from node_details import NodeDetailsFromOuput


class Cluster:
    def __init__(self, number_of_shards=0, replicas_per_shard=0, ip = DEFAULT_IP, start_port=REDIS_START_PORT,
                 use_auth=False, password=None):
        self.number_of_shards = number_of_shards
        self.replicas_per_shard = replicas_per_shard
        self.use_auth = use_auth
        self.ip = ip
        self.start_port = start_port
        self.password = self.get_password(password)
        self.cluster_path = get_current_path() + REDIS_CONFIG_FOLDER
        self.total_nodes = self.number_of_shards + self.replicas_per_shard * self.number_of_shards

    def get_password(self, password):
        """
        Returns password if set by user when creating cluster else returns a
        randomly generated password.
        """
        if self.use_auth and password:
            return password
        elif self.use_auth and not password:
            return generate_password()

    def create_cluster(self):

        # Remove the Existing directory if already present
        if is_directory_present(self.cluster_path):
            remove_directory(self.cluster_path)

        # Create the cluster directory
        create_directory(self.cluster_path)

        cluster_instance = ''
        for counter in range(self.total_nodes):
            curr_node_port = self.start_port + counter

            curr_cluster_node = ClusterNode(cluster_path=self.cluster_path, node_port=curr_node_port,
                                            password=self.password)
            curr_cluster_node.create_node()
            curr_cluster_node.start_node()
            socket_address = self.ip + ':' + str(curr_node_port)
            cluster_instance += socket_address + ' '

        # Create the cluster Command
        create_cluster_cmd = 'redis-cli --cluster create' + ' ' + cluster_instance + ' ' + '--cluster-replicas ' + \
                             str(self.replicas_per_shard) + ' --cluster-yes'
        if self.use_auth:
            create_cluster_cmd += ' -a {}'.format(self.password)

        run_subprocess([create_cluster_cmd])

    def get_cluster_info(self):
        if self.password:
            cmd = REDIS_CLI_CMD + str(self.start_port) + ' -a ' + self.password + ' --no-auth-warning cluster nodes'
        else:
            cmd = 'redis-cli -p ' + str(self.start_port) + ' cluster nodes'

        process = run_subprocess([cmd], PIPE, None)

        output = process.communicate()[0]
        node_details_list = NodeDetailsFromOuput()
        node_info_list = node_details_list.get_details(output, self.total_nodes, self.password)
        return node_info_list

    def destroy_cluster(self):
        
        # Shutdown of all the nodes present in a cluster
        for counter in range(self.total_nodes):
            #Current port Number
            curr_node_port = self.start_port + counter

            curr_cluster_node = ClusterNode(cluster_path=self.cluster_path, node_port=curr_node_port,
                                                     password=self.password)
            #Shutdown of current Node through Redis Cli command
            curr_cluster_node.destroy_node()

        #Remove the cluster configuration file
        remove_file(get_current_path()+CLUSTER_CONFIG_PATH)

    def cluster_information(self):

        #Check if cluster configuration file already present or not
        #If present remove that file
        if is_file_present(get_current_path() + CLUSTER_CONFIG_PATH):
             remove_file(get_current_path()+CLUSTER_CONFIG_PATH)

       #Dump all the cluster information into a json file

        cluster_info = {
             NUMBER_OF_SHARDS: self.number_of_shards,
             REPLICAS_PER_SHARD: self.replicas_per_shard,
             USER_AUTH:str(self.use_auth),
             PASSWORD : str(self.password) if self.password else EMPTY_STRING,
             START_PORT:REDIS_START_PORT
        }
        json_string = json.dumps(cluster_info, indent=4)
        json_file = open_file(get_current_path()+CLUSTER_CONFIG_PATH,FILE_MODE_WRITE)
        write_on_file(json_file, json_string)
        close_file(json_file)

