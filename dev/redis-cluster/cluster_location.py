from enum import Enum

class ClusterLocation(Enum):
    '''Enum to indicate where a cluster lies'''
    ELASTICACHE = "elasticache"
    LOCAL = "local"
