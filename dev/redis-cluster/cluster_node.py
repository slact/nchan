from constants import APPEND_ONLY, CLUSTER_CONFIG_FILE, CLUSTER_ENABLED, CLUSTER_NODE_TIMEOUT
from constants import REDIS_CONFIG_FILE_NAME, START_REDIS_SERVER_CMD, REDIS_CONFIG_FOLDER, REDIS_START_PORT,REDIS_CLI_CMD,\
        SHUTDOWN_CMD,AUTHENTICATION_CMD
from utils import change_directory
from utils import join_path_components, create_directory, run_subprocess, get_current_path
from utils import open_file, write_on_file


class ClusterNode:

    def __init__(self, cluster_path, node_port, password=None):
        self.cluster_path = cluster_path
        self.node_port = node_port
        self.password = password

    def create_node(self):
        node_path = join_path_components(self.cluster_path, str(self.node_port))
        create_directory(node_path)
        redis_config_path = join_path_components(node_path, REDIS_CONFIG_FILE_NAME)
        self.create_node_config(redis_config_path)

    def create_node_config(self, redis_config_path):
        file = open_file(file_path=redis_config_path, mode='a')
        self.add_node_config(file)

    def start_node(self):

        curr_path = join_path_components(self.cluster_path, str(self.node_port))
        change_directory(curr_path)
        run_subprocess([START_REDIS_SERVER_CMD])

    def destroy_node(self):

        #Redis Cli shutdown Command to destroy node
        if self.password:
            run_subprocess([REDIS_CLI_CMD+str(self.node_port)+AUTHENTICATION_CMD+self.password+SHUTDOWN_CMD])
        else:
            run_subprocess([REDIS_CLI_CMD+str(self.node_port)+SHUTDOWN_CMD])

    def add_node_config(self, file):
        write_on_file(file, "port " + str(self.node_port))
        write_on_file(file, "cluster-enabled " + CLUSTER_ENABLED)
        write_on_file(file, "cluster-config-file " + CLUSTER_CONFIG_FILE)
        write_on_file(file, "cluster-node-timeout " + CLUSTER_NODE_TIMEOUT)
        if self.password:
            write_on_file(file, "requirepass {}".format(self.password))
            write_on_file(file, "masterauth {}".format(self.password))
        write_on_file(file, "appendonly " + APPEND_ONLY)

    def get_node_password(self):
        node_path = join_path_components(self.cluster_path, str(REDIS_START_PORT))
        redis_config_path = join_path_components(node_path, REDIS_CONFIG_FILE_NAME)
        file_pass=open(redis_config_path,"r")
        lines=file_pass.read().splitlines()
        password = lines[4].split(' ')
        if (password[0] == "requirepass"):
            return password[1]
