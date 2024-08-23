#!/usr/bin/env python3
import calendar
import shutil

import pandas as pd
import json
import zipfile

import warnings
warnings.filterwarnings("ignore")

import tempfile
import subprocess
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pathlib
sns.set_style('darkgrid')
from matplotlib.ticker import EngFormatter
import argparse
import logging
import os

import datetime
import gzip

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

    parser = argparse.ArgumentParser(description='Helper script to download all data files from https://www.gharchive.org/')
    parser.add_argument('output_dir', help="output directory where to store files.")

    args = parser.parse_args()

    logging.info(f"Preparing download of gharchive files to {args.output_dir}")
    os.makedirs(args.output_dir, exist_ok=True)

    today = datetime.date.today()
    start_date = datetime.date(year=2011, month=2, day=12)
    urls = []
    for year in range(2011, today.year+1):
        if year < 2016:
            continue

        #for month in range(1, 13):
        # download only october for now.
        for month in [10]:
            single_month_urls = []
            for day in range(1, 32):
                try:
                    d = datetime.date(year=year, month=month, day=day)
                except:
                    continue # gets rid off potentially invalid dates
                if d >= start_date and d < today:
                    for hour in range(0, 24):
                        url = f'https://data.gharchive.org/{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz'
                        single_month_urls.append(url)
            if single_month_urls:
                urls.append(single_month_urls)
    num_urls = sum(len(url) for url in urls)
    logging.info(f"Prepared {num_urls:,} urls ({start_date} to {today}) to download.")

    # Use parallel curl (requires at least version 7.68)

    # write temporary file with urls etc out
    # format of this file should be
    # url = https://data.gharchive.org/2015-02-12-0.json.gz
    # output = {output_dir}/2015-02-12-0.json.gz
    # ...

    # Download a single month completely, then process files to output folders.
    cache_dir = os.path.join(args.output_dir, "cache")
    daily_output_dir = os.path.join(args.output_dir, "daily")

    for batch_no, url_batch in enumerate(sorted(urls)):
        os.makedirs(cache_dir, exist_ok=True)
        month_name = '/'.join(os.path.basename(url_batch[0]).split('-')[:2])
        logging.info(f"Downloading {batch_no + 1}/{len(urls)} -- {month_name} -- {len(url_batch)} files")
        with tempfile.NamedTemporaryFile("w") as fp:
            for url in url_batch:
                fp.write(f'url = {url}\n')
                fp.write(f'output = {os.path.join(cache_dir, os.path.basename(url))}\n')
            fp.flush()

            # invoke curl process (much faster than some python solution)
            # curl --parallel --parallel-immediate --config test.txt
            logging.info(f"Wrote urls to temporary file {fp.name}")
            subprocess.run(['wc', '-l', fp.name], check=True)
            # subprocess.run(['cat', fp.name], check=True)
            subprocess.run(['curl', '--parallel', '--parallel-immediate', '--config', fp.name], check=True)

        # create daily files from download.
        os.makedirs(daily_output_dir, exist_ok=True)
        year, month = os.path.basename(url_batch[0]).split('-')[:2]
        year, month = int(year), int(month)
        days_per_month = [calendar.monthrange(year, m)[1] for m in range(1, 13)]

        logging.info("Creating daily json files from data.")
        total_size = 0
        for day in range(1, days_per_month[month - 1] + 1):
            output_path = os.path.join(daily_output_dir, f'{year:04d}-{month:02d}-{day:02d}.json.gz')
            with gzip.open(output_path, 'w') as fp:
                for hour in range(0, 24):
                    input_path = os.path.join(cache_dir, f'{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz')
                    try:
                        if os.path.isfile(input_path):
                            with gzip.open(input_path, 'r') as gp:
                                content = gp.read()
                                fp.write(content)
                    except Exception as e:
                        logging.error(f"Error processing {input_path}, details: {e}. Skipping file.")
            zipped_size = os.stat(output_path).st_size
            logging.info(f'Wrote {int(zipped_size / 1024 / 1024):,} MiB to {output_path}')
            total_size += zipped_size
        logging.info(f"Storing {year:04d}-{month:02d} as json.gz uses {int(zipped_size / 1024 / 1024):,} MiB in total")

        # remove files from cache dir
        shutil.rmtree(cache_dir)


    # TODO: could store in bzip2 as this is splittable https://www.kurokatta.org/grumble/2021/03/splittable-bzip2.