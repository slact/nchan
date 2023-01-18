import argparse
from cluster import Cluster
from constants import REDIS_START_PORT,CLUSTER_CONFIG_PATH,CLUSTER_SHUTDOWN_WARN,FILE_MODE_READ,NUMBER_OF_SHARDS,\
            USER_AUTH,PASSWORD,REPLICAS_PER_SHARD
from utils import get_current_path,is_file_present,open_file,load_json,get_cluster_configuration


def destroy_cluster_via_redis_cli(config_list):

    #Create Cluster Object
    cluster = Cluster(number_of_shards=int(config_list[NUMBER_OF_SHARDS]), replicas_per_shard=int(config_list[REPLICAS_PER_SHARD]),
                                use_auth=config_list[USER_AUTH],password=config_list[PASSWORD])

    #Warning Message for Cluster Shutdown
    destroy_cluster = input (CLUSTER_SHUTDOWN_WARN+'\n')

    #Check for Cluster Shutdown
    if destroy_cluster=='Y' or destroy_cluster=='y':
        cluster.destroy_cluster()

if __name__ == "__main__":
    #Check For cluster Information file present or not
    if(is_file_present(get_current_path()+CLUSTER_CONFIG_PATH)):
        #Get all the required cluster information
        config_list=get_cluster_configuration(CLUSTER_CONFIG_PATH,FILE_MODE_READ)
        if config_list:
            #Call for destroy cluster
            destroy_cluster_via_redis_cli(config_list)






