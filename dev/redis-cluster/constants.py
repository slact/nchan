REDIS_CONFIG_FOLDER = '/redis-configs'
REDIS_START_PORT = 6379
REDIS_CONFIG_FILE_NAME = 'redis.conf'
CLUSTER_ENABLED = 'yes'
CLUSTER_CONFIG_FILE = 'nodes.conf'
CLUSTER_NODE_TIMEOUT = '5000'
APPEND_ONLY = 'yes'
START_REDIS_SERVER_CMD = 'redis-server ./redis.conf'
DEFAULT_IP = '127.0.0.1'
REDIS_CLI_CMD = 'redis-cli -p '
CLUSTER_NODES_INFO = 'redis-cli -p ' + str(REDIS_START_PORT) + ' cluster nodes'
CLUSTER_CONFIG_PATH='/cluster_information.json'
CLUSTER_SHUTDOWN_WARN="The process will shut down and will lose all the data to process press Y , to cancel press any button apart from Y"
FILE_MODE_APPEND='a'
FILE_MODE_READ='r'
FILE_MODE_WRITE='w'
NUMBER_OF_SHARDS='number_of_shards'
REPLICAS_PER_SHARD='replicas_per_shard'
USER_AUTH='user_auth'
PASSWORD='password'
SHUTDOWN_CMD=' shutdown'
AUTHENTICATION_CMD=' -a '
NO_AUTH_WARNING=' --no-auth-warning'
CLUSTER_NODES_CMD=' cluster nodes'
EMPTY_STRING=""
START_PORT='start_port'
CLUSTER_STATE_INFORMATION='/cluster_state_info.txt'
READING_MODE = 'r'
CONFIG_FILE_PATH = './config.json'
KEYWORD_CLUSTER = 'cluster'
ARG_PREFIX = '--'
ARG_KEYWORD_AUTH = 'auth'
ARG_KEYWORD_START_PORT = 'start_port'
ARG_KEYWORD_SHARDS = 'shards'
ARG_KEYWORD_REPLICAS = 'replicas'
ARG_KEYWORD_IP = 'ip'
ARG_KEYWORD_PASSWORD= 'password'
ARG_KEYWORD_NUMNODEGROUPS = 'numnodegroups'
ARG_KEYWORD_REPLICASPERNODEGROUP = 'replicaspernodegroup'
ARG_KEYWORD_CACHENODETYPE = 'cachenodetype'
ARG_KEYWORD_ENGINEVERSION = 'engineversion'
ARG_KEYWORD_REPLICATIONGROUPDESCRIPTION = 'replicationgroupdescription'
ARG_KEYWORD_REPLICATIONGROUPID = 'replicationgroupid'
ARG_KEYWORD_RETAINPRIMARYCLUSTER = 'retainprimarycluster'
CLUSTER_SHUTDOWN_CMD = 'ps -ef | grep redis-server | awk \'{print $2}\' | xargs kill -9'
REDIS_ENGINE = 'redis'
ELASTICACHE_PARSER='elasticache'
LOCAL_PARSER='local'
ARGS_NUM = 5
CONFIG_DEFAULT_STRING = '{"shards": 3, "replicas": 1, "auth": "False", "ip": "127.0.0.1", "start_port": 6379}'
CLUSTER_SHUTDOWN_CMD = 'ps -ef | grep redis-server | awk \'{print $2}\' | xargs kill -9'