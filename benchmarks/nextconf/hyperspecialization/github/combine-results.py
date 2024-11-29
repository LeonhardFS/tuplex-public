#!/usr/bin/env python3
import logging
from datetime import datetime
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

def preprocess_python_df(df_tplx):
    df = df_tplx.copy()
    #df = df.dropna()
    df.drop(columns='input_path', inplace=True)
    columns_to_keep = ['run', 'benchmark', 'input_path',
                       'total_time_in_s', 'mode', 'output_path', 'startup_time_in_s']
    df = df.explode('per_file_stats')
    df.reset_index(drop=True, inplace=True)

    def expand_helper(t):
        if pd.isna(t) or t is None:
            return pd.Series()
        data = {'input_row_count': t['num_input_rows'],
                'output_row_count': t['num_output_rows'],
                'time_in_s': t['duration'],
                'input_path': t['input_path']
                }

        return pd.Series(list(data.values()), index=(data.keys()))
    df = pd.merge(df, df['per_file_stats'].apply(expand_helper), left_index=True, right_index=True)

    df['mode'] = 'python'
    df['loading_time_in_s'] = np.nan
    df['total_time_in_s'] = df['job_time_in_s']
    cols = ['mode', 'input_path', 'output_path', 'time_in_s', 'loading_time_in_s',
            'total_time_in_s', 'input_row_count', 'output_row_count']
    return df[cols]

def load_python(zf):
    df = pd.read_json(zf.open('python_baseline.ndjson'), lines=True)
    return preprocess_python_df(df)

def load_python(path):
    run = os.path.basename(path).split('.')[0]
    run = int(run[run.find('run-')+4:])

    df = pd.read_json(path, lines=True)
    df.drop(columns='input_path', inplace=True)
    columns_to_keep = ['run', 'benchmark', 'input_path',
                       'total_time_in_s', 'mode', 'output_path', 'startup_time_in_s']
    df = df.explode('per_file_stats')
    df.reset_index(drop=True, inplace=True)

    def expand_helper(t):
        if pd.isna(t) or t is None:
            return pd.Series()
        data = {'input_row_count': t['num_input_rows'],
                'output_row_count': t['num_output_rows'],
                'time_in_s': t['duration'],
                'input_path': t['input_path']
                }

        return pd.Series(list(data.values()), index=(data.keys()))
    df = pd.merge(df, df['per_file_stats'].apply(expand_helper), left_index=True, right_index=True)

    df['mode'] = 'python'
    df['run'] = run
    df['loading_time_in_s'] = np.nan
    df['total_time_in_s'] = df['job_time_in_s']
    cols = ['run', 'mode', 'input_path', 'output_path', 'time_in_s', 'loading_time_in_s',
            'total_time_in_s', 'input_row_count', 'output_row_count']
    return df[cols]

def extract_per_file_stats_tuplex(job_stats):
    n_requests = len(job_stats['responses'])
    L = [job_stats['responses'][i]['rowStats'] for i in range(n_requests)]
    for i in range(n_requests):
        req_uri = job_stats['tasks'][i]['input_uris'][0]
        L[i]['input_path'] = req_uri[:req_uri.rfind(':')]
        L[i]['req_uri'] = req_uri
        L[i]['duration'] = job_stats['responses'][i]['taskExecutionTime']
        inp = job_stats['requests'][i]['input_paths_taken']
        outp = job_stats['requests'][i]['output_paths_taken']
        L[i]['num_input_rows'] = inp['normal'] + inp['general'] + inp['fallback'] + inp['unresolved']
        L[i]['num_output_rows'] = outp['normal'] + outp['unresolved']

        # request time and hyperspecialization time
        L[i]['request_time_in_s'] = (datetime.utcfromtimestamp(job_stats['requests'][i]['tsRequestEnd'] / 1000000000) - datetime.utcfromtimestamp(job_stats['requests'][i]['tsRequestStart'] / 1000000000)).total_seconds()
        L[i]['hyperspecialization_time'] = job_stats['requests'][i].get('t_hyper')

    return L

# expand per_file_stats in df
def expand_tuplex_df(df_tplx):
    df = df_tplx.copy()
    #df = df.dropna()
    df.drop(columns='input_path', inplace=True)
    columns_to_keep = ['run', 'benchmark', 'input_path',
                       'total_time_in_s', 'mode', 'output_path', 'startup_time_in_s']
    df = df.explode('per_file_stats')
    df.reset_index(drop=True, inplace=True)

    def expand_helper(t):
        if pd.isna(t) or t is None:
            return pd.Series()
        # example looks like this
        # {'hyper_active': True,
        #   'input': {'fallback': 0,
        #    'general': 0,
        #    'input_file_count': 1,
        #    'normal': 48899,
        #    'total_input_row_count': 48899,
        #    'unresolved': 0},
        #   'output': {'except': 0, 'normal': 1418},
        #   'request_total_time': 0.743892436,
        #   'spills': {'count': 0, 'size': 0},
        #   'timings': {'compile_time': 0.0298965,
        #    'fast_path_execution_time': 0.413112,
        #    'general_and_interpreter_time': 4.3911e-05,
        #    'hyperspecialization_time': 0.279614},
        #   'input_path': '/hot/data/github_daily/2011-10-15.json',
        #   'req_uri': '/hot/data/github_daily/2011-10-15.json:0-78478920',
        #   'duration': 0.743892436,
        #   'num_input_rows': 48899,
        #   'num_output_rows': 1418}
        data = {'input_row_count': t['num_input_rows'],
                'output_row_count': t['num_output_rows'],
                'time_in_s': t['duration'],
                'input_path': t['input_path']
                }
        if t.get('hyper_active') is not None:
            # deprecated code, can likely remove...
            more_data = {'hyper': t.get('hyper_active'),
                         'request_time_in_s': t['request_total_time'],
                         'compile_time_in_s': t['timings'].get('compile_time'),
                         'fast_path_execution_time_in_s':t['timings'].get('fast_path_execution_time'),
                         'general_and_interpreter_time_in_s':t['timings'].get('general_and_interpreter_time'),
                         'hyperspecialization_time':t['timings'].get('hyperspecialization_time'),}
            data.update(more_data)
        else:
            data['request_time_in_s'] = t['request_time_in_s']
            data['hyperspecialization_time'] = t['hyperspecialization_time']
        return pd.Series(list(data.values()), index=(data.keys()))
    df = pd.merge(df, df['per_file_stats'].apply(expand_helper), left_index=True, right_index=True)
    df.drop(columns=['options', 'scratch_path'], inplace=True)

    return df

def load_tuplex(path, mode):
    with open(path, 'r') as fp:
        lines = fp.readlines()
        rows = [json.loads(line) for line in lines]

        data = []
        for row in rows:
            ans = {'benchmark': 'github'}
            for k in ['benchmark', 'input_path', 'job_time_in_s', 'metrics', 'mode', 'options', 'output_path', 'scratch_path', 'startup_time_in_s']:
                ans[k] = row.get(k)
            try:
                ans['per_file_stats'] = extract_per_file_stats_tuplex(row['detailed_job_stats'])
            except Exception as e:
                print(f'--- ERR: extract failed for path {path}')
                raise e
            row = ans
            data.append(row)
        df = pd.DataFrame(data).reset_index().rename(columns={'index':'run', 'job_time_in_s':'total_time_in_s'})

        run = os.path.basename(path).split('.')[0]
        run = int(run[run.find('run-')+4:])
        df['run'] = run

        df['mode'] = mode
        return expand_tuplex_df(df)


def load_and_combine_results_to_df(experimental_result_root_path):
    if not os.path.isdir(experimental_result_root_path):
        raise ValueError(f'path {experimental_result_root_path} is not a valid directory')

    # step 1: load python files
    df = pd.DataFrame()

    logging.info('Loading python baseline.')
    for path in glob.glob(os.path.join(experimental_result_root_path, 'python', 'log-run-*.ndjson')):
        path_df = load_python(path)
        df = pd.concat((df, path_df))

    # step 2:
    logging.info('Loading Tuplex runs.')
    tuplex_modes = [os.path.basename(d) for d in glob.glob(os.path.join(experimental_result_root_path, 'tuplex-*')) if os.path.isdir(d)]
    logging.info(f'Found {len(tuplex_modes)} modes ({tuplex_modes}) to load.')

    for mode in tuplex_modes:
        logging.info(f'Loading tuplex mode {mode}')
        for path in glob.glob(os.path.join(experimental_result_root_path, mode, 'log-run-*.ndjson')):
            path_df = load_tuplex(path, mode)
            assert len(path_df) != 0
            df = pd.concat((df, path_df))

    # Step 3: cleanup
    df['benchmark'] = 'github'

    return df

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

if __name__ == '__main__':

    setup_logging()

    parser = argparse.ArgumentParser(description='Helper script to combine results into single ndjson file')
    parser.add_argument('input_path', help="input root of where results of experimental runs are stored.")
    parser.add_argument('output_path', help="output_path of combined result file.")

    args = parser.parse_args()

    df = load_and_combine_results_to_df(args.input_path)

    # Store result
    logging.info(f"Storing combined result to {args.output_path}")
    df.to_json(args.output_path, orient='records', lines=True, index=False)

