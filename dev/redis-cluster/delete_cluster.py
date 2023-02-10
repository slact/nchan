import argparse
import logging
import boto3
from cluster_location import ClusterLocation
from cluster_shutdown_by_rediscli import shutdown_cluster
from cluster_start import ArgumentRequirementState
from constants import ARG_KEYWORD_REPLICATIONGROUPID, ARG_KEYWORD_RETAINPRIMARYCLUSTER, ARG_PREFIX
from utils import add_cli_args_to_arg_parser

logging.basicConfig(level=logging.INFO)

def delete_elasticache_cluster(args):
    client = boto3.client('elasticache')
    response = client.delete_replication_group(
            ReplicationGroupId=args.replicationgroupid,
            RetainPrimaryCluster=args.retainprimarycluster,
    )
    logging.info(response)

def get_args_for_elasticache_cluster(elasticache_parser, main_parser):
    ec_cli_arguments = [
        ((ARG_PREFIX+ARG_KEYWORD_REPLICATIONGROUPID,), {'type':str, 'required':ArgumentRequirementState.REQUIRED.value}),
        ((ARG_PREFIX+ARG_KEYWORD_RETAINPRIMARYCLUSTER,), {'action':argparse.BooleanOptionalAction, 'required':ArgumentRequirementState.REQUIRED.value}),
    ]

    # Arguments provided from command line are added to Argument Parser. 
    elasticache_parser = add_cli_args_to_arg_parser(elasticache_parser, ec_cli_arguments)
    return main_parser

def get_delete_cluster_args():
    '''Reads arguments from command line'''
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='location')
    local = subparser.add_parser('local')
    elasticache = subparser.add_parser('elasticache')
    parser = get_args_for_elasticache_cluster(elasticache, parser)
    args = parser.parse_args()
    return args

if __name__ == '__main__':  
    args = get_delete_cluster_args()
    if args.location == ClusterLocation.LOCAL.value:
        shutdown_cluster()
    elif args.location == ClusterLocation.ELASTICACHE.value:
        delete_elasticache_cluster(args)
    else:
        print("Insufficient Arguments: Please mention which cluster to delete.")
    