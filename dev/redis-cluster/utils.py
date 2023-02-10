import pathlib
import os
import shutil
import uuid
import subprocess
import json

from constants import READING_MODE


def get_current_path():
    return str(pathlib.Path().resolve())


def is_directory_present(path):
    return os.path.exists(path) and os.path.isdir(path)


def remove_directory(path):
    shutil.rmtree(path)


def create_directory(path):
    os.mkdir(path)


def change_directory(path):
    os.chdir(path)

def is_file_present(path):
	 return os.path.exists(path)


def remove_file(path):
	 os.remove(path)


def join_path_components(path1, path2):
    return os.path.join(path1, path2)


def generate_password():
    '''
    Returns a randomly generated password i.e. a random
    UUID as a 32-character lowercase hexadecimal string
    '''
    return uuid.uuid4().hex


def open_file(file_path, mode):
    return open(file_path, mode)


def write_on_file(file, data):
    file.write(data + "\n")

def close_file(file):
	     file.close()


def load_json(file):
    return json.load(file)



def dump_json(list):
    return json.dumps(list,indent=4)


def run_subprocess(cmd_, stdout=None, stderr=None):
	     return subprocess.Popen(cmd_, stdout = stdout,  stderr = stderr, shell=True)


def get_cluster_configuration(path,mode):
    file=open_file(get_current_path()+path,mode)
    config_list=load_json(file)
    return config_list



def load_json(json_str):
    '''
        Converts and returns JSON as python dict from given JSON string
    '''
    try:
        return json.loads(json_str)
    except ValueError as e:
        print(e)
        return None

def read_json_file(file_path):
    '''
        Returns JSON as python dict after reading from given JSON file
    '''
    try:
        with open(file_path, READING_MODE) as file:
            return load_json(file.read())
    except FileNotFoundError:
        print('WARNING: The file `{}` could not be found.'.format(file_path))
        return None
    except OSError as e:
        print(e)


def str2bool(bool_str):
    '''
    Converts string value to boolean value
    '''
    return bool_str.lower() in ("yes", "true", "t", "1", "y")

def add_cli_args_to_arg_parser(parser, cli_args):
    '''
    Arguments provided from command line are added to Argument Parser
    '''
    for arg, options in cli_args:
        parser.add_argument(*arg, **options)
    return parser

def get_cluster_info_using_args(args):
    if args.auth:
        cmd = REDIS_CLI_CMD + str(args.start_port) + ' -a ' + str(args.password) + ' cluster nodes'
        cluster_info = subprocess.check_output([cmd])
    else:
        cluster_info = subprocess.check_output([CLUSTER_NODES_INFO])
    
    return cluster_info
    