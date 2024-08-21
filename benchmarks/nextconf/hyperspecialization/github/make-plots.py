#!/usr/bin/env python3
import pandas as pd
import json
import zipfile

import warnings
warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pathlib
sns.set_style('darkgrid')
from matplotlib.ticker import EngFormatter
import argparse
import logging
import os

SAVE_FIG_DPI=180

def compute_df_totals(df):
    # ignore first warm-up/verification run
    df = df[df['run'] != 0].copy()

    part_df_A = df.groupby(['run', 'mode'])[['hyperspecialization_time', 'request_time_in_s']].sum()
    # combine with total runtime
    part_df_B = df.groupby(['run', 'mode'])[['total_time_in_s']].first()

    df_totals = pd.concat((part_df_A, part_df_B), axis=1).reset_index()

    df_totals = df_totals.sort_values(by='total_time_in_s').reset_index(drop=True)

    return df_totals

def plot_scatter(df, path):
    df_totals = compute_df_totals(df)

    modes = df_totals['mode'].unique()

    for i, mode in enumerate(modes):
        y = df_totals[df_totals['mode'] == mode]['total_time_in_s']
        x = [i] * len(y)
        plt.scatter(y, x, linewidth=2, marker='x')
    ax = plt.gca()
    ax.set_yticklabels(modes)
    plt.yticks(range(len(modes)), rotation=0)
    ax.xaxis.set_major_formatter(EngFormatter(unit='s', sep=''))
    plt.grid(axis='y')
    plt.title('hyper vs. global\n(same optimizations applied)')

    os.makedirs(pathlib.Path(path).parent, exist_ok=True)
    plt.savefig(path, dpi=SAVE_FIG_DPI, bbox_inches='tight', pad_inches=0.5, transparent=True)


def plot_breakdown(df, path):

    df_totals = compute_df_totals(df)

    # 10th Percentile
    def q10(x):
        return x.quantile(0.5)

    # 90th Percentile
    def q90(x):
        return x.quantile(0.9)

    gdf = df_totals.groupby('mode').agg(['mean', 'std', q10, q90]).reset_index()

    gdf[('other_time_in_s', 'mean')] = gdf[('total_time_in_s', 'mean')] - gdf[('request_time_in_s', 'mean')]
    gdf[('compute_time_in_s', 'mean')] = gdf[('request_time_in_s', 'mean')] - gdf[('hyperspecialization_time', 'mean')]
    gdf = gdf.sort_values(by=('total_time_in_s', 'mean')).reset_index(drop=True)

    plt.figure(figsize=(10,5))
    kwargs={'edgecolor':None, 'lw':0}

    h = gdf[('compute_time_in_s', 'mean')]
    plt.barh(gdf.index, h, **kwargs, label='time spent on compute on individual requests')
    hh = gdf[('hyperspecialization_time', 'mean')]
    plt.barh(gdf.index, hh, left=h, **kwargs, label='time spent on hyperspecialization')
    hhh = gdf[('other_time_in_s', 'mean')]
    handle = plt.barh(gdf.index, hhh, left=h + hh, **kwargs, label='time spent on client (global optimization/compilation)')
    # modify python handle
    handle[-1].set_color([.5]*3)
    # plot total error bar
    mu = gdf[('total_time_in_s', 'mean')]
    sigma = gdf[('total_time_in_s', 'std')]
    q10 = gdf[('total_time_in_s', 'q10')]
    q90 = gdf[('total_time_in_s', 'q90')]

    for lower,upper,y in zip(q10,q90,range(len(q10))):
        plt.plot((lower,upper),(y,y),'ro-',color=[0.2]*3, markersize=3)

    plt.scatter(mu, gdf.index, 40, marker='o', color='k',zorder=10)

    # print runtime numbers in s
    for xi, yi in zip(mu, range(len(mu))):
        plt.text(xi + 1.5, yi + .2, f'{xi:.1f}s', horizontalalignment='left', verticalalignment='center')

    modes = list(gdf['mode'])
    ax = plt.gca()
    ax.set_yticklabels(modes)
    plt.yticks(range(len(modes)), rotation=0)
    ax.xaxis.set_major_formatter(EngFormatter(unit='s', sep=''))

    plt.text(85, 1, 'Optimizations used\nfor all settings:\n- filter-promotion\n- constant-folding\n- selection-pushdown\n- simplify-large-structs\n  (except for global-structs)', fontsize = 10,
             bbox = dict(facecolor = 'white', alpha = 0.5))

    plt.grid(axis='y')
    plt.title('Breakdown: hyper vs. global\n(same optimizations applied)')
    plt.xlim(0, 160)
    plt.legend(loc='best')
    plt.tight_layout()

    os.makedirs(pathlib.Path(path).parent, exist_ok=True)
    plt.savefig(path, dpi=SAVE_FIG_DPI, bbox_inches='tight', pad_inches=0.5, transparent=True)
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
    parser.add_argument('input_path', help="input path of combined result ndjson file (use combine-results.py to get it).")
    parser.add_argument('output_dir', help="output directory where to store plots.")

    args = parser.parse_args()

    logging.info("Reading data.")
    df = pd.read_json(args.input_path, lines=True)

    logging.info("Plotting scatter of totals.")
    plot_scatter(df, os.path.join(args.output_dir, 'tuplex-hyper-vs-global-scatter.png'))

    logging.info("Plotting breakdown.")
    plot_breakdown(df, os.path.join(args.output_dir, 'tuplex-hyper-vs-global-breakdown.png'))
