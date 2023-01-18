from enum import Enum
import logging
import argparse
import boto3
from configparser import ConfigParser
from cluster import Cluster
from cluster_location import ClusterLocation
from constants import *
from utils import add_cli_args_to_arg_parser, read_json_file, str2bool

logging.basicConfig(level=logging.INFO)

class ArgumentRequirementState(Enum):
    '''Enum to indicate the requirement of arguments from command line'''
    REQUIRED = True
    UNREQUIRED = False

def get_args_from_config():
    '''Reads arguments from Config file. Default argument values to be set from config file, if file exists.
    Get default argument values (stored as constants), if config does not exist'''
    config_parser = ConfigParser()
    config = read_json_file(CONFIG_FILE_PATH)
    if config:
        config_parser.read_dict(config)
        config_local = dict(config_parser.items(ClusterLocation.LOCAL.value))
        config_elasticache = dict(config_parser.items(ClusterLocation.ELASTICACHE.value))
        return config_local, config_elasticache
    return {},{}

def get_ec_arg_requirement_state(config_args):
    NumNodeGroups_required = ReplicasPerNodeGroup_required = CacheNodeType_required = EngineVersion_required = ReplicationGroupId_required = ReplicationGroupDescription_required = ArgumentRequirementState.UNREQUIRED
    '''Get the requirement of command line arguments for elasticache cluster based on args in config'''
    if(ARG_KEYWORD_NUMNODEGROUPS not in config_args):
        NumNodeGroups_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_REPLICASPERNODEGROUP not in config_args):
        ReplicasPerNodeGroup_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_CACHENODETYPE not in config_args):
        CacheNodeType_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_ENGINEVERSION not in config_args):
        EngineVersion_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_REPLICATIONGROUPDESCRIPTION not in config_args):
        ReplicationGroupDescription_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_REPLICATIONGROUPID not in config_args):
        ReplicationGroupId_required = ArgumentRequirementState.REQUIRED

    return NumNodeGroups_required, ReplicasPerNodeGroup_required, EngineVersion_required, ReplicationGroupId_required, ReplicationGroupDescription_required, CacheNodeType_required

def get_local_arg_requirement_state(config_args):
    '''Get the requirement of command line arguments for local cluster based on args in config'''
    shards_required = replicas_required = auth_required = ip_required = start_port_required = ArgumentRequirementState.UNREQUIRED
    if(ARG_KEYWORD_SHARDS not in config_args):
        shards_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_REPLICAS not in config_args):
        replicas_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_AUTH not in config_args):
        auth_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_IP not in config_args):
        ip_required = ArgumentRequirementState.REQUIRED
    if(ARG_KEYWORD_START_PORT not in config_args):
        start_port_required = ArgumentRequirementState.REQUIRED

    return shards_required, replicas_required, auth_required, ip_required, start_port_required

def get_args_for_local_cluster(config_args, local_parser, parser):
    '''Reads arguments for local cluster from command line'''
    if config_args:
        config_args[ARG_KEYWORD_AUTH]=str2bool(config_args[ARG_KEYWORD_AUTH]) # Argument Parser accepts boolean values for auth argument
        local_parser.set_defaults(**config_args) # Set default arguments values in parser in case they aren't provided through command line
        shards_required, replicas_required, auth_required, ip_required, start_port_required = get_local_arg_requirement_state(config_args)

    # Requirement of command line args depends on the availability of args from config or defaults
    local_cli_arguments = [
        ((ARG_PREFIX+ARG_KEYWORD_SHARDS,), {'type':int, 'required':shards_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_REPLICAS,), {'type':int, 'required':replicas_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_AUTH,), {'action':argparse.BooleanOptionalAction, 'required':auth_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_PASSWORD,), {'type':str, 'required':ArgumentRequirementState.UNREQUIRED.value}),
        ((ARG_PREFIX+ARG_KEYWORD_IP,), {'type':str, 'required':ip_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_START_PORT,), {'type':int, 'required':start_port_required.value})
    ]

    # Arguments provided from command line are added to Argument Parser. 
    local_parser = add_cli_args_to_arg_parser(local_parser, local_cli_arguments)
    return parser

def get_args_for_elasticache_cluster(config_args, elasticache_parser, parser):
    '''Reads arguments for elasticache cluster from command line'''
    if config_args:
        elasticache_parser.set_defaults(**config_args) # Set default arguments values in parser in case they aren't provided through command line
        NumNodeGroups_required, ReplicasPerNodeGroup_required, EngineVersion_required, ReplicationGroupId_required, ReplicationGroupDescription_required, CacheNodeType_required = get_ec_arg_requirement_state(config_args)

    # Requirement of command line args depends on the availability of args from config or defaults
    ec_cli_arguments = [
        ((ARG_PREFIX+ARG_KEYWORD_NUMNODEGROUPS,), {'type':int, 'required':NumNodeGroups_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_REPLICASPERNODEGROUP,), {'type':int, 'required':ReplicasPerNodeGroup_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_CACHENODETYPE,), {'type':str, 'required':CacheNodeType_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_ENGINEVERSION,), {'type':str, 'required':EngineVersion_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_REPLICATIONGROUPID,), {'type':str, 'required':ReplicationGroupId_required.value}),
        ((ARG_PREFIX+ARG_KEYWORD_REPLICATIONGROUPDESCRIPTION,), {'type':str, 'required':ReplicationGroupDescription_required.value})
    ]

    # Arguments provided from command line are added to Argument Parser. 
    elasticache_parser = add_cli_args_to_arg_parser(elasticache_parser, ec_cli_arguments)
    return parser

def get_args_from_cmd_line(config_local, config_elasticache):
    '''Reads arguments from command line'''
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='location')
    local = subparser.add_parser(LOCAL_PARSER)
    elasticache = subparser.add_parser(ELASTICACHE_PARSER)
    parser = get_args_for_local_cluster(config_local, local, parser)
    parser = get_args_for_elasticache_cluster(config_elasticache, elasticache, parser)
    args = parser.parse_args()
    return args

def get_cluster_args():
    config_local, config_elasticache = get_args_from_config()
    args = get_args_from_cmd_line(config_local, config_elasticache)
    return args

def create_and_start_cluster(args):
    #creates an object cluster
    cluster = Cluster(number_of_shards=args.shards, replicas_per_shard=args.replicas, ip=args.ip, start_port=args.start_port, 
                        use_auth=args.auth, password=args.password)

    #starts cluster
    cluster.create_cluster()
    return cluster

def create_elasticache_cluster(args):
    ''' Creates a cluster mode enabled cluster with (NumNodeGroups) shards, 
    1 primary node (implicit) and (replicasPerNodeGroup) replicas '''
    client = boto3.client('elasticache')
    response = client.create_replication_group(
        AutomaticFailoverEnabled=True,
        CacheNodeType=args.cachenodetype,
        Engine=REDIS_ENGINE,
        EngineVersion=args.engineversion,
        ReplicationGroupDescription=args.replicationgroupdescription,
        ReplicationGroupId=args.replicationgroupid,
        NumNodeGroups=args.numnodegroups,
        ReplicasPerNodeGroup=args.replicaspernodegroup,
    )
    logging.info(response)

if __name__ == '__main__':  
    args = get_cluster_args()
    if args.location == ClusterLocation.LOCAL.value:
        create_and_start_cluster(args)
    elif args.location == ClusterLocation.ELASTICACHE.value:
        create_elasticache_cluster(args)
    else:
        print("Insufficient Arguments: Please mention where you want to run the cluster.")
            