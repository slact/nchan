from subprocess import PIPE
from utils import run_subprocess
from constants import REDIS_CLI_CMD


class Node:

    def __init__(self, node_details, password):
        self.port_number = self.get_curr_node(node_details)
        self.node_id = self.get_node_id(node_details)
        self.ip = self.get_node_ip(node_details)
        self.role = self.get_node_role(node_details, password)
        self.status = self.get_status_of_node(node_details)

    def get_curr_node(self, node_details):
        try:
            ip_address = node_details[1].split(':')
            return ip_address[1].split('@')[0]
        except IndexError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None

    def get_node_id(self, node_details):
        try:
            return node_details[0]
        except IndexError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None

    def get_node_ip(self, node_details):
        try:
            ip_port = node_details[1].split(':')
            return ip_port[0]
        except IndexError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None

    def get_node_role(self, node_details, password):
        try:
            status_of_node = node_details[7]
            if status_of_node == "connected":
                if password:
                    cmd = REDIS_CLI_CMD + self.get_curr_node(
                        node_details) + ' -a ' + password + " --no-auth-warning info | grep ^role"
                    ps = run_subprocess([cmd], PIPE)
                else:
                    cmd = REDIS_CLI_CMD + self.get_curr_node(node_details) + " info | grep ^role"
                    ps = run_subprocess([cmd], PIPE)
                node_role = ps.communicate()[0].decode("utf-8").splitlines()
                node_role = node_role[0].split(':')[1]
            else:
                node_role = node_details[2].split(',')[0]
            return node_role

        except IndexError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None

    def get_status_of_node(self, node_details):
        try:
            return node_details[7]
        except IndexError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None


class NodeDetailsFromOuput:
    def get_details(self, cluster_info_output, total_nodes, password):
        cluster_info_output = cluster_info_output.decode("utf-8").splitlines()
        node_info_list = []
        for nodes in range(total_nodes):
            node_details = cluster_info_output[nodes].split(' ')
            node_info = Node(node_details, password)
            node_info_list.append(node_info)
        return node_info_list
