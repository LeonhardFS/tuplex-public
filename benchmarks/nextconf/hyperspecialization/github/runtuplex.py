#!/usr/bin/env python3
# this script holds a (dummy) pipeline for working with Lambda/S3/Github data

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse

def extract_repo_id_code(row):
    if 2012 <= row['year'] <= 2014:
        return row['repository']['id']
    else:
        return row['repo']['id']

def fork_event_pipeline(ctx, input_pattern, s3_output_path):
    """test pipeline to extract fork evenets across years"""
    ctx.json(input_pattern) \
        .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
        .withColumn('repo_id', extract_repo_id_code) \
        .filter(lambda x: x['type'] == 'ForkEvent') \
        .selectColumns(['type', 'repo_id', 'year']) \
        .tocsv(s3_output_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github hyper specialization query')
    # parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
    #                     help='path or pattern to zillow data')
    # parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
    #                     help='specify path where to save output data files')
    # parser.add_argument('--single-threaded', dest='single_threaded', action="store_true",
    #                     help="whether to use a single thread for execution")
    # parser.add_argument('--preload', dest='preload', action="store_true",
    #                     help="whether to add a cache statement after the csv operator to separate IO costs out.")
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    args = parser.parse_args()

    #if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
    #    raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 2
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/github-exp"
    use_hyper_specialization = not args.no_hyper
    use_constant_folding = not args.no_cf
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv'
    s3_output_path = 's3://tuplex-leonhard/experiments/flights_github'

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'

    # two options: full dataset or tiny sample
    input_pattern = 's3://tuplex-public/data/github_daily/*.json' # <-- full dataset

    # small sample
    #input_pattern = 's3://tuplex-public/data/github_daily_sample/*.sample'

    input_pattern = 's3://tuplex-public/data/github_daily/2011*.json,s3://tuplex-public/data/github_daily/2013*.json' # <-- single file   


    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    # deactivate, b.c. not supported yet...
    # print('    constant-folding: {}'.format(use_constant_folding))
    # print('    null-value optimization: {}'.format(not args.no_nvo))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'tuplex.aws.lambdaThreads': 0,
            'tuplex.aws.verboseLogging':True,
            'tuplex.csv.maxDetectionMemory': '256KB',
            "aws.scratchDir": s3_scratch_dir,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "csv.selectionPushdown" : True}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    # for github deactivate all the optimizations, so stuff runs
    conf["optimizer.constantFoldingOptimization"] = False
    conf["optimizer.filterPushdown"] = False
    conf["optimizer.selectionPushdown"] = False

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    fork_event_pipeline(ctx, input_pattern, s3_output_path)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))