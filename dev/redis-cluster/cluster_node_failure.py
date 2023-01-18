import argparse
import boto3
from cluster_location import ClusterLocation
from constants import ARG_KEYWORD_REPLICATIONGROUPID, ARG_PREFIX, ARG_KEYWORD_NODEGROUPID, ARG_KEYWORD_PORTNUMBER, REDIS_CLI_CMD
from utils import add_cli_args_to_arg_parser
from cluster_start import ArgumentRequirementState
import logging
from utils import run_subprocess
from subprocess import PIPE


logging.basicConfig(level=logging.INFO)

'''
Automatic failover related events in order of occurrance --
Replication group message: Test Failover API called for node group <node-group-id>
Cache cluster message: Failover from primary node <primary-node-id> to replica node <node-id> completed
Replication group message: Failover from primary node <primary-node-id> to replica node <node-id> completed
Cache cluster message: Recovering cache nodes <node-id>
Cache cluster message: Finished recovery for cache nodes <node-id>
'''


def local_node_failover(args):
    port_number = args.portnumber
    cmd = REDIS_CLI_CMD + port_number + " info | grep ^role"
    ps = run_subprocess([cmd], PIPE)
    node_role = ps.communicate()[0].decode("utf-8").splitlines()
    node_role = node_role[0].split(':')[1]
    if node_role == 'master':
        primary_node_failure = './primary_node_failure.sh'
        run_subprocess([primary_node_failure])
    else:
        slave_node_failure = './slave_node_failure.sh'
        run_subprocess([slave_node_failure])


def elasticache_node_group_failover(args):
    # method called to perform failover on a node-group in a cluster
    client = boto3.client('elasticache')

    response = client.test_failover(
        ReplicationGroupId=args.replicationgroupid,
        NodeGroupId=args.nodegroupid
    )

    logging.info(response)


def get_args_for_elasticache_cluster(elasticache_parser, main_parser):
    ec_cli_arguments = [
        ((ARG_PREFIX + ARG_KEYWORD_REPLICATIONGROUPID,), {'type': str, 'required': ArgumentRequirementState.REQUIRED.value}),
        ((ARG_PREFIX + ARG_KEYWORD_NODEGROUPID,), {'type': str, 'required': ArgumentRequirementState.REQUIRED.value})
    ]

    # Arguments provided from command line are added to Argument Parser.
    elasticache_parser = add_cli_args_to_arg_parser(elasticache_parser, ec_cli_arguments)
    return main_parser


def get_args_for_local_cluster(local_parser, main_parser):
    local_cli_arguments = [
        ((ARG_PREFIX + ARG_KEYWORD_PORTNUMBER,), {'type' : str, 'required': ArgumentRequirementState.REQUIRED.value})
    ]
    local_parser = add_cli_args_to_arg_parser(local_parser, local_cli_arguments)
    return main_parser


def get_args_from_cmd_line():
    '''Reads arguments from command line'''
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='location')
    local = subparser.add_parser('local')
    elasticache = subparser.add_parser('elasticache')
    parser = get_args_for_local_cluster(local, parser)
    parser = get_args_for_elasticache_cluster(elasticache, parser)
    args = parser.parse_args()
    return args


def get_cluster_args():
    args = get_args_from_cmd_line()
    return args


if __name__ == '__main__':
    args = get_cluster_args()
    if args.location == ClusterLocation.LOCAL.value:
        local_node_failover(args)
    elif args.location == ClusterLocation.ELASTICACHE.value:
        elasticache_node_group_failover(args)
    else:
        print("Insufficient Arguments: Please mention which node-group to fail.")
