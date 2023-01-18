from utils import run_subprocess, get_current_path, write_on_file, open_file, close_file
from cluster_node import ClusterNode
from constants import REDIS_CLI_CMD, CLUSTER_NODES_INFO, REDIS_CONFIG_FOLDER, REDIS_START_PORT,\
        NO_AUTH_WARNING,CLUSTER_NODES_CMD,AUTHENTICATION_CMD,FILE_MODE_WRITE,CLUSTER_STATE_INFORMATION


def get_cluster_info():
    info_file = open_file(get_current_path()+CLUSTER_STATE_INFORMATION,FILE_MODE_WRITE)

    cluster_node = ClusterNode(cluster_path=get_current_path() + REDIS_CONFIG_FOLDER, node_port=REDIS_START_PORT)
    password = cluster_node.get_node_password()
    if password:
        cmd = REDIS_CLI_CMD + str(REDIS_START_PORT) +AUTHENTICATION_CMD+ str(password) +NO_AUTH_WARNING+CLUSTER_NODES_CMD
        a = run_subprocess([cmd], info_file, info_file)
    else:
        a = run_subprocess([CLUSTER_NODES_INFO], info_file, info_file)
    write_on_file(info_file, 'NODE INFO:')

    close_file(info_file)


if __name__ == "__main__":
    get_cluster_info()
