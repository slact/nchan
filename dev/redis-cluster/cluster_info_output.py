from utils import open_file, write_on_file, close_file,is_file_present,get_current_path, get_cluster_configuration
from constants import CLUSTER_CONFIG_PATH, FILE_MODE_READ, FILE_MODE_WRITE,REPLICAS_PER_SHARD,USER_AUTH,PASSWORD,\
        NUMBER_OF_SHARDS
import json
from cluster import Cluster

def get_cluster_information(config_list):

    #Create cluster object
    cluster = Cluster(number_of_shards=int(config_list[NUMBER_OF_SHARDS]), replicas_per_shard=int(config_list[REPLICAS_PER_SHARD]),
                            use_auth=config_list[USER_AUTH],password=config_list[PASSWORD])

    node_info_list = cluster.get_cluster_info()

    json_string = json.dumps([z.__dict__ for z in node_info_list], indent=4)
    json_file = open_file('{}'.format("data.json"), FILE_MODE_WRITE)
    write_on_file(json_file, json_string)
    close_file(json_file)

if __name__ == "__main__":
    #Check For cluster Information file present or not
    if(is_file_present(get_current_path()+CLUSTER_CONFIG_PATH)):
        #Get all the required cluster information
        config_list=get_cluster_configuration(CLUSTER_CONFIG_PATH,FILE_MODE_READ)
        if config_list:
            #Store all the Cluster Information in json file
            get_cluster_information(config_list)