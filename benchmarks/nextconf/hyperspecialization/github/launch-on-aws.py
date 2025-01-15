#!/usr/bin/env python3

import sys
import argparse
import logging
import boto3
import time
import json
import os
import atexit

from botocore.config import Config
from dateutil.parser import parser

# Global AWS config. Change here for different regions.
global_aws_config = Config(
    region_name = 'us-east-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)
# Use Ubuntu 24.04 image.
global_ami_id = 'ami-0866a3c8686eaeeba'
global_ec2_instance_type = 'm5.large'
# Use aws ec2 describe-security-groups to get ids from name
# global_security_group = 'all-ips'
global_security_group = "sg-0445039f7f4641a50"
global_vpc_name = 'experiments-vpc'

# Use aws ec2 describe-subnets to get ids from name
# global_subnet_id = 'experiments-subnet-public2-us-east-1b'
global_subnet_id = "subnet-0761387e05610a8fe"


# helper variable to hold active instances which need to be shutdown when script exits.
global_active_instances = set()


def setup_logging() -> None:

    LOG_FORMAT="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    LOG_DATE_FORMAT="%d/%b/%Y %H:%M:%S"

    handlers = []

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(LOG_FORMAT)
    # tell the handler to use this format
    console.setFormatter(formatter)

    handlers.append(console)

    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATE_FORMAT,
                        handlers=handlers)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num-runs', type=int, dest='num_runs', default=1,
                        help='How many times each configuration should run')
    parser.add_argument('--ssh-key', dest='ssh_key', default='~/.ssh/tuplex.pem', help='SSH key to use for launching EC2 instances')
    parser.add_argument('--use-on-demand', dest='use_on_demand', action='store_true', default=False, help="Whether to use on-demand instances or not (default: spot instances, because cheaper)")
    parser.add_argument('-b', '--branch', dest='branch', default='lambda-exp-llvm16',
                        help='Specify which branch to use from tuplex repo.')
    return parser.parse_args()

def launch_ec2_instance(args: argparse.Namespace):
    logging.info(f'Launching single EC2 coordinator instance (type={global_ec2_instance_type}, ami={global_ami_id}).')

    ssh_key_path = os.path.expandvars(os.path.expanduser(args.ssh_key))

    # Check that ssh key file exists
    assert os.path.isfile(ssh_key_path), f"SSH key file {args.ssh_key} not existing."

    ec2 = boto3.client('ec2', config=global_aws_config)

    response = ec2.run_instances(ImageId=global_ami_id, InstanceType=global_ec2_instance_type,
                                 NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0, 'SubnetId': global_subnet_id, 'Groups': [global_security_group]}],
                                 MinCount=1, MaxCount=1,
                                 DryRun=True)

    print(response)



# def start_instance(conn, image_id='ami-0226c1fcd5f7f2898', instance_type='r5d.4xlarge'):
#     cur_instances = []
#
#     cache_file_path = 'tuplex_instances.json'
#
#     if os.path.isfile(cache_file_path):
#         for line in open(cache_file_path).readlines():
#             inst = json.loads(line)
#             if inst not in cur_instances:
#                 cur_instances.append(inst)
#
#     instances_file = open(cache_file_path, 'w')
#
#     instances = []
#     spot_requests = conn.request_spot_instances(price='2.00', image_id=image_id, count=1, key_name='tuplex',
#                                                 security_groups=['allips'],
#                                                 instance_type=instance_type, user_data='')
#     assert len(spot_requests) > 0
#     spot_request = spot_requests[0]
#
#     # wait until requests get fulfilled...
#     open_request = True
#     while open_request:
#         time.sleep(5)
#         open_request = False
#         spot_requests = conn.get_all_spot_instance_requests(request_ids=[spot_request.id])
#         for spot_request in spot_requests:
#             print('Spot request status: {}'.format(spot_request.state))
#             if spot_request.state == 'open':
#                 open_request = True
#
#     # Get info about instances
#     instance_num = 0
#     reservations = conn.get_all_reservations()
#     for reservation in reservations:
#         for instance in reservation.instances:
#
#             # skip updates for spot requests not beloning to the currently active ones
#             if instance.spot_instance_request_id not in [spot_request.id]:
#                 continue
#
#             instance.update()
#             while instance.state == u'pending':
#                 time.sleep(1)
#                 instance.update()
#             if instance.state == u'running':
#                 if instance.id not in cur_instances:
#                     instance_num += 1
#                     print('Started instance {}: {}'.format(instance_num, instance))
#                     desc = {'public_dns_name': instance.public_dns_name, 'id': instance.id}
#                     instances.append(desc)
#                     instances_file.write(json.dumps(desc) + '\n')
#             else:
#                 print('Could not start instance: {}'.format(instance))
#     instances_file.close()
#     return instances

def main():
    setup_logging()
    logging.info("Welcome to running Viton/Tuplex experiments on AWS.")
    args = parse_args()

    launch_ec2_instance(args)

    logging.info("Experiment script done.")

if __name__ == "__main__":
    main()