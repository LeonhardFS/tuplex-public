#!/usr/bin/env python3
import logging
from typing import Optional
import os
import pathlib
import argparse
import pandas as pd
import json
import zipfile
import os
import glob
import logging
import numpy as np

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


def load_and_combine_results_to_df(result_dir):
    rows = []
    modes = sorted([os.path.basename(path) for path in glob.glob(os.path.join(result_dir, '*'))])
    for mode in modes:
        mode_root_dir = os.path.join(result_dir, mode)
        run_files = glob.glob(os.path.join(mode_root_dir, '*.ndjson'))
        for path in run_files:
            with open(path, 'r') as fp:
                data = json.loads(fp.readlines()[-1])

            # Add entry
            row = {'mode': mode, 'benchmark': data['benchmark'], 'total_time_in_s': data['job_time_in_s']}

            row['cost_in_USD'] = data['detailed_job_stats']['cost']

            # Extract how much time was spent on lambda.
            lambda_time = sum([x['durationInMs'] for x in data['detailed_job_stats']['requests']])
            lambda_billed_time = sum([x['billedDurationInMs'] for x in data['detailed_job_stats']['requests']])

            # Add invoked requests on top of this (one-level for now).
            for r in data['responses']:
                if 'invokedRequests' in r:
                    for i_r in r['invokedRequests']:
                        lambda_time += i_r['timings']['durationInMs']
                        lambda_billed_time += i_r['timings']['billedDurationInMs']

            row['total_lambda_time_in_s'] = lambda_time / 1000.0
            row['total_lambda_billed_time_in_s'] = lambda_billed_time / 1000.0

            # Extract total time aggregates for lambda
            lambda_compile_time = sum([x['t_compile'] for x in data['detailed_job_stats']['requests']])
            lambda_fast_time = sum([x['t_fast'] for x in data['detailed_job_stats']['requests']])
            lambda_slow_time = sum([x['t_slow'] for x in data['detailed_job_stats']['requests']])
            lambda_hyper_time = sum([x.get('t_hyper') for x in data['detailed_job_stats']['requests']])

            row['total_lambda_compile_time_in_s'] = lambda_compile_time
            row['total_lambda_fast_path_time_in_s'] = lambda_fast_time
            row['total_lambda_slow_path_time_in_s'] = lambda_slow_time
            row['total_lambda_hyperspecialization_time_in_s'] = lambda_hyper_time

            # Compute the paths taken (across lambdas)
            total_normal_row_count = data['detailed_job_stats']['input_paths_taken']['normal']
            total_fallback_row_count = data['detailed_job_stats']['input_paths_taken']['fallback']
            total_general_row_count = data['detailed_job_stats']['input_paths_taken']['general']
            total_unresolved_row_count = data['detailed_job_stats']['input_paths_taken']['unresolved']
            total_output_row_count = data['detailed_job_stats']['output_paths_taken']['normal']

            row[
                'input_row_count'] = total_fallback_row_count + total_general_row_count + total_normal_row_count + total_unresolved_row_count
            row['output_row_count'] = total_output_row_count
            row['normal_count'] = total_normal_row_count
            row['general_count'] = total_general_row_count
            row['fallback_count'] = total_fallback_row_count
            row['unresolved_count'] = total_unresolved_row_count

            # Compute the time spent (issuing/waiting) on lambda requests. That helps deduct the client-time
            tsRequestStartMin = min([x['tsRequestStart'] for x in data['detailed_job_stats']['requests']])
            tsRequestStartMax = max([x['tsRequestEnd'] for x in data['detailed_job_stats']['requests']])

            lambda_request_time_in_s = (tsRequestStartMax - tsRequestStartMin) / 10 ** 9
            row['total_request_time_in_s'] = lambda_request_time_in_s
            row['total_client_time_in_s'] = row['total_time_in_s'] - lambda_request_time_in_s

            # Compute how many containers were newly initialized, reused and what number of containers were used.
            container_ids = []
            reuse_count = 0
            n_count = 0
            for task in data['detailed_job_stats']['tasks']:
                container_ids.append(task['container']['uuid'])
                reuse_count += task['container']['reused']
                n_count += 1 + len(task['invoked_containers'])
                for self_c in task['invoked_containers']:
                    container_ids.append(self_c['uuid'])
                    reuse_count += self_c['reused']
            row['total_container_count'] = len(set(container_ids))
            row['total_reuse_count'] = reuse_count
            row['total_request_count'] = n_count

            # Detected/sampled normal schemas.
            normal_schemas = [x['input_schemas']['normal'] for x in data['detailed_job_stats']['requests']]

            for task in data['detailed_job_stats']['tasks']:
                for self_c in task['invoked_requests']:
                    normal_schemas.append(self_c['input_schemas']['normal'])

            # Remove uninitialized.
            unique_normal_schemas = set(normal_schemas) - {'uninitialized'}

            # JSON dump.
            row['unique_normal_schemas'] = json.dumps(sorted(list(unique_normal_schemas)))
            row['unique_normal_schemas_count'] = len(unique_normal_schemas)

            # run:
            run = os.path.basename(path).split('.')[0]
            run = int(run[run.find('run-') + 4:])
            row['run'] = run

            rows.append(row)

    return pd.DataFrame(rows)

if __name__ == '__main__':

    setup_logging()

    parser = argparse.ArgumentParser(description='Helper script to combine Tuplex on Lambda results into single csv file.')
    parser.add_argument('input_path', help="input root of where results of experimental runs are stored.")
    parser.add_argument('output_path', help="output_path of combined result file.")

    args = parser.parse_args()

    df = load_and_combine_results_to_df(args.input_path)

    # Store result
    logging.info(f"Storing combined result to {args.output_path}")
    df.to_csv(args.output_path, index=False)

