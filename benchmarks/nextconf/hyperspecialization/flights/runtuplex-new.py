#!/usr/bin/env python3
# flights query for Viton paper
# TODO: build with cmake -G "Unix Makefiles" -DBUILD_WITH_CEREAL=ON -DSKIP_AWS_TESTS=OFF -DBUILD_WITH_ORC=OFF -DBUILD_WITH_AWS=ON -DPython3_EXECUTABLE=/home/leonhards/.pyenv/shims/python3.9 -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=/opt/llvm-9 ..

# invoke python baseline via --mode python --input-pattern "/hot/data/flights_all/*.csv" --output-path "./local-exp/python-baseline/flights/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/python-baseline/flights/results.ndjson"

import logging
import pathlib
from typing import Optional

import time
import sys
import json
import os
import glob
import argparse
import logging
import csv
# used for validation
import pandas as pd

# frameworks supported are
# - python baseline
# - Tuplex
# - lithops
# - ??

# default parameters to use for paths, scratch dirs
S3_DEFAULT_INPUT_PATTERN="s3://tuplex-public/data/flights_all/*.csv"
S3_DEFAULT_OUTPUT_PATH='s3://tuplex-leonhard/experiments/flights'
S3_DEFAULT_SCRATCH_DIR="s3://tuplex-leonhard/scratch/flights-exp"

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"

def make_year_range(num_years):
    """
    helper function to create year range for year based experiment
    """
    if num_years <= 1:
        return [2003]
    start_year = max(1987, 2003 - (num_years - 1) // 2)
    end_year = start_year + num_years

    years = list(range(start_year, end_year))[:num_years]
    assert len(years) == num_years and 2003 in years
    return years

def extract_feature_vector(row):
    carrier_list = [None, 'EA', 'UA', 'PI', 'NK', 'PS', 'AA', 'NW', 'EV', 'B6', 'HP', 'TW', 'DL', 'OO', 'F9', 'YV',
                    'TZ', 'US',
                    'MQ', 'OH', 'HA', 'ML (1)', 'XE', 'G4', 'YX', 'DH', 'AS', 'KH', 'QX', 'CO', 'FL', 'VX', 'PA (1)',
                    'WN', '9E']

    airport_list = [None, 'ABE', 'ABI', 'ABQ', 'ABR', 'ABY', 'ACK', 'ACT', 'ACV', 'ACY', 'ADK', 'ADQ', 'AEX', 'AGS',
                    'AKN',
                    'ALB', 'ALO', 'ALS', 'ALW', 'AMA', 'ANC', 'ANI', 'APF', 'APN', 'ART', 'ASE', 'ATL', 'ATW', 'ATY',
                    'AUS', 'AVL', 'AVP', 'AZA', 'AZO', 'BDL', 'BET', 'BFF', 'BFI', 'BFL', 'BFM', 'BGM', 'BGR', 'BHM',
                    'BIL', 'BIS', 'BJI', 'BKG', 'BLI', 'BLV', 'BMI', 'BNA', 'BOI', 'BOS', 'BPT', 'BQK', 'BQN', 'BRD',
                    'BRO', 'BRW', 'BTM', 'BTR', 'BTV', 'BUF', 'BUR', 'BWI', 'BZN', 'CAE', 'CAK', 'CBM', 'CCR', 'CDB',
                    'CDC', 'CDV', 'CEC', 'CGI', 'CHA', 'CHO', 'CHS', 'CIC', 'CID', 'CIU', 'CKB', 'CLD', 'CLE', 'CLL',
                    'CLT', 'CMH', 'CMI', 'CMX', 'CNY', 'COD', 'COS', 'COU', 'CPR', 'CRP', 'CRW', 'CSG', 'CVG', 'CWA',
                    'CYS', 'DAB', 'DAL', 'DAY', 'DBQ', 'DCA', 'DDC', 'DEC', 'DEN', 'DET', 'DFW', 'DHN', 'DIK', 'DLG',
                    'DLH', 'DRO', 'DRT', 'DSM', 'DTW', 'DUT', 'DVL', 'EAR', 'EAT', 'EAU', 'ECP', 'EFD', 'EGE', 'EKO',
                    'ELM', 'ELP', 'ENV', 'ERI', 'ESC', 'EUG', 'EVV', 'EWN', 'EWR', 'EYW', 'FAI', 'FAR', 'FAT', 'FAY',
                    'FCA', 'FLG', 'FLL', 'FLO', 'FMN', 'FNL', 'FNT', 'FOD', 'FOE', 'FSD', 'FSM', 'FWA', 'GCC', 'GCK',
                    'GCN', 'GEG', 'GFK', 'GGG', 'GJT', 'GLH', 'GNV', 'GPT', 'GRB', 'GRI', 'GRK', 'GRR', 'GSO', 'GSP',
                    'GST', 'GTF', 'GTR', 'GUC', 'GUM', 'HDN', 'HGR', 'HHH', 'HIB', 'HKY', 'HLN', 'HNL', 'HOB', 'HOU',
                    'HPN', 'HRL', 'HSV', 'HTS', 'HVN', 'HYA', 'HYS', 'IAD', 'IAG', 'IAH', 'ICT', 'IDA', 'IFP', 'ILE',
                    'ILG', 'ILM', 'IMT', 'IND', 'INL', 'IPL', 'IPT', 'ISN', 'ISO', 'ISP', 'ITH', 'ITO', 'IYK', 'JAC',
                    'JAN', 'JAX', 'JFK', 'JLN', 'JMS', 'JNU', 'JST', 'KOA', 'KSM', 'KTN', 'LAN', 'LAR', 'LAS', 'LAW',
                    'LAX', 'LBB', 'LBE', 'LBF', 'LBL', 'LCH', 'LCK', 'LEX', 'LFT', 'LGA', 'LGB', 'LIH', 'LIT', 'LMT',
                    'LNK', 'LNY', 'LRD', 'LSE', 'LWB', 'LWS', 'LYH', 'MAF', 'MAZ', 'MBS', 'MCI', 'MCN', 'MCO', 'MCW',
                    'MDT', 'MDW', 'MEI', 'MEM', 'MFE', 'MFR', 'MGM', 'MHK', 'MHT', 'MIA', 'MIB', 'MKC', 'MKE', 'MKG',
                    'MKK', 'MLB', 'MLI', 'MLU', 'MMH', 'MOB', 'MOD', 'MOT', 'MQT', 'MRY', 'MSN', 'MSO', 'MSP', 'MSY',
                    'MTH', 'MTJ', 'MVY', 'MWH', 'MYR', 'OAJ', 'OAK', 'OGD', 'OGG', 'OGS', 'OKC', 'OMA', 'OME', 'ONT',
                    'ORD', 'ORF', 'ORH', 'OTH', 'OTZ', 'OWB', 'OXR', 'PAE', 'PAH', 'PBG', 'PBI', 'PDX', 'PFN', 'PGD',
                    'PGV', 'PHF', 'PHL', 'PHX', 'PIA', 'PIB', 'PIE', 'PIH', 'PIR', 'PIT', 'PLN', 'PMD', 'PNS', 'PPG',
                    'PRC', 'PSC', 'PSE', 'PSG', 'PSM', 'PSP', 'PUB', 'PUW', 'PVD', 'PVU', 'PWM', 'RAP', 'RCA', 'RDD',
                    'RDM', 'RDR', 'RDU', 'RFD', 'RHI', 'RIC', 'RIW', 'RKS', 'RNO', 'ROA', 'ROC', 'ROP', 'ROR', 'ROW',
                    'RST', 'RSW', 'SAF', 'SAN', 'SAT', 'SAV', 'SBA', 'SBN', 'SBP', 'SCC', 'SCE', 'SCK', 'SDF', 'SEA',
                    'SFB', 'SFO', 'SGF', 'SGU', 'SHD', 'SHR', 'SHV', 'SIT', 'SJC', 'SJT', 'SJU', 'SKA', 'SLC', 'SLE',
                    'SLN', 'SMF', 'SMX', 'SNA', 'SOP', 'SPI', 'SPN', 'SPS', 'SRQ', 'STC', 'STL', 'STS', 'STT', 'STX',
                    'SUN', 'SUX', 'SWF', 'SWO', 'SYR', 'TBN', 'TEX', 'TKI', 'TLH', 'TOL', 'TPA', 'TRI', 'TTN', 'TUL',
                    'TUP', 'TUS', 'TVC', 'TVL', 'TWF', 'TXK', 'TYR', 'TYS', 'UCA', 'UIN', 'USA', 'UST', 'UTM', 'VCT',
                    'VEL', 'VIS', 'VLD', 'VPS', 'WRG', 'WYS', 'XNA', 'XWA', 'YAK', 'YAP', 'YKM', 'YNG', 'YUM']

    state_list = [None, 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
                  'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                  'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
                  'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico',
                  'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
                  'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
                  'U.S. Pacific Trust Territories and Possessions', 'U.S. Virgin Islands', 'Utah', 'Vermont',
                  'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

    # invalid -> return 0-based feature vector.
    if row['CRS_ARR_TIME'] is None or row['CRS_DEP_TIME'] is None:
        return [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    # categorical variables
    quarter = row['QUARTER']
    month = row['MONTH']
    day_of_month = row['DAY_OF_MONTH']
    day_of_week = row['DAY_OF_WEEK']
    carrier = carrier_list.index(row['OP_UNIQUE_CARRIER'])
    origin_airport = airport_list.index(row['ORIGIN'])
    dest_airport = airport_list.index(row['DEST'])

    origin_state = state_list.index(row['ORIGIN_STATE_NM'])
    dest_state = state_list.index(row['DEST_STATE_NM'])

    # numerical variables
    dep_delay = row['DEP_DELAY']
    arr_delay = row['ARR_DELAY']

    crs_arr_hour = float(int(row['CRS_ARR_TIME']) // 100)
    crs_dep_hour = float(int(row['CRS_DEP_TIME']) // 100)
    crs_arr_5min = float(int(row['CRS_ARR_TIME']) % 100 // 5)
    crs_dep_5min = float(int(row['CRS_DEP_TIME']) % 100 // 5)

    features = [float(quarter), float(month), float(day_of_month),
                float(day_of_week), float(carrier), float(origin_state),
                float(dest_state), dep_delay, arr_delay,
                crs_arr_hour, crs_dep_hour,
                crs_arr_5min, crs_dep_5min]

    return features

def fill_in_delays(row):
    # want to fill in data for missing carrier_delay, weather delay etc.
    # only need to do that prior to 2003/06

    year = row['YEAR']
    month = row['MONTH']
    arr_delay = row['ARR_DELAY']

    if year == 2003 and month < 6 or year < 2003:
        # fill in delay breakdown using model and complex logic
        if arr_delay is None:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': None,
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 0.:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 5.:
            # it's an ontime flight, just attribute any delay to the carrier
            carrier_delay = arr_delay
            # set the rest to 0
            # ....
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : carrier_delay,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        else:
            # use model to determine everything and set into (join with weather data?)
            # i.e., extract here a couple additional columns & use them for features etc.!

            # use here linear model
            # (for now random numbers, but can easily fit some from the data)
            # features is a list of 13 values.
            f = row['features']
            mu = [ 10.34020663,  -5.32110325, -23.76501655,  -9.9471319 ,
                   11.08306402,  28.44419468,  34.33795322,  -3.5271869 ,
                   14.31617576,  -2.37601258,   3.10218378,  31.74930251,
                   30.65958678]

            std = [ 6.14500234, 15.43086255,  3.95305606,  8.95857769,  5.82440163,
                    10.0310225 ,  8.20700403,  8.16668611,  3.34533295,  9.30209542,
                    7.54420077,  0.17705991, 13.322939  ]
            normalized_f = [(f[0] - mu[0]) / std[0], (f[1] - mu[1]) / std[1], (f[2] - mu[2]) / std[2], (f[3] - mu[3]) / std[3], (f[4] - mu[4]) / std[4], (f[5] - mu[5]) / std[5], (f[6] - mu[6]) / std[6], (f[7] - mu[7]) / std[7], (f[8] - mu[8]) / std[8], (f[9] - mu[9]) / std[9], (f[10] - mu[10]) / std[10], (f[11] - mu[11]) / std[11], (f[12] - mu[12]) / std[12]]

            normalized_t_carrier_delay = normalized_f[0] * 1.12 + normalized_f[1] * 4.49 + normalized_f[2] * 3.42 + normalized_f[3] * 4.28 + normalized_f[4] * 0.81 + normalized_f[5] * 5.13 + normalized_f[6] * 5.26 + normalized_f[7] * -0.07 + normalized_f[8] * -1.39 + normalized_f[9] * 2.74 + normalized_f[10] * -2.93 + normalized_f[11] * 4.81 + normalized_f[12] * 4.36
            t_carrier_delay = normalized_t_carrier_delay * 4.06 + 1.48

            normalized_t_weather_delay = normalized_f[0] * 1.84 + normalized_f[1] * -1.92 + normalized_f[2] * 2.59 + normalized_f[3] * -1.49 + normalized_f[4] * 9.29 + normalized_f[5] * 7.88 + normalized_f[6] * 1.11 + normalized_f[7] * 3.87 + normalized_f[8] * 0.57 + normalized_f[9] * 4.32 + normalized_f[10] * 7.65 + normalized_f[11] * -4.69 + normalized_f[12] * 0.07
            t_weather_delay = normalized_t_weather_delay * -5.07 + 5.95

            normalized_t_nas_delay = normalized_f[0] * -3.33 + normalized_f[1] * 9.12 + normalized_f[2] * 2.64 + normalized_f[3] * 0.05 + normalized_f[4] * 2.51 + normalized_f[5] * -3.30 + normalized_f[6] * -1.44 + normalized_f[7] * -2.38 + normalized_f[8] * 6.60 + normalized_f[9] * 2.86 + normalized_f[10] * 2.74 + normalized_f[11] * 3.00 + normalized_f[12] * 2.61
            t_nas_delay = normalized_t_nas_delay * 5.90 + 5.87

            normalized_t_security_delay = normalized_f[0] * 4.25 + normalized_f[1] * -0.68 + normalized_f[2] * 3.40 + normalized_f[3] * 5.63 + normalized_f[4] * 3.57 + normalized_f[5] * 3.19 + normalized_f[6] * 2.90 + normalized_f[7] * 1.43 + normalized_f[8] * 3.86 + normalized_f[9] * 4.24 + normalized_f[10] * 10.60 + normalized_f[11] * 0.84 + normalized_f[12] * 2.97
            t_security_delay = normalized_t_security_delay * 0.11 + 8.68

            normalized_t_late_aircraft_delay = normalized_f[0] * 2.89 + normalized_f[1] * -1.12 + normalized_f[2] * 1.52 + normalized_f[3] * 3.10 + normalized_f[4] * 4.74 + normalized_f[5] * -0.97 + normalized_f[6] * 1.47 + normalized_f[7] * 8.53 + normalized_f[8] * 0.63 + normalized_f[9] * 6.67 + normalized_f[10] * 4.44 + normalized_f[11] * 3.12 + normalized_f[12] * 2.92
            t_late_aircraft_delay = normalized_t_late_aircraft_delay * -4.50 + 1.04

            # the arrival delay is more than 5min, now scale such that numbers add up!
            s_factor = t_carrier_delay + t_weather_delay + t_nas_delay + t_security_delay + t_late_aircraft_delay
            if abs(s_factor) < 0.001:
                s_factor = 1.0
            s_factor = arr_delay / s_factor
            t_carrier_delay *= s_factor
            t_weather_delay *= s_factor
            t_nas_delay *= s_factor
            t_security_delay *= s_factor
            t_late_aircraft_delay *= s_factor

            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : t_carrier_delay,
                    'weather_delay': t_weather_delay,
                    'nas_delay' : t_nas_delay,
                    'security_delay': t_security_delay,
                    'late_aircraft_delay' : t_late_aircraft_delay}
    else:
        # just return it as is
        return {'year' : year, 'month' : month,
                'day' : row['DAY_OF_MONTH'],
                'carrier': row['OP_UNIQUE_CARRIER'],
                'flightno' : row['OP_CARRIER_FL_NUM'],
                'origin': row['ORIGIN_AIRPORT_ID'],
                'dest': row['DEST_AIRPORT_ID'],
                'distance' : row['DISTANCE'],
                'dep_delay' : row['DEP_DELAY'],
                'arr_delay': row['ARR_DELAY'],
                'carrier_delay' : row['CARRIER_DELAY'],
                'weather_delay':row['WEATHER_DELAY'],
                'nas_delay' : row['NAS_DELAY'],
                'security_delay': row['SECURITY_DELAY'],
                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}

def glob_paths(pattern):
    paths = []
    for pattern_part in pattern.split(','):
        paths += sorted(glob.glob(pattern_part))
    return paths

# special conversion function for boolean necessary
def python_baseline_to_bool(value):
    valid = {'true': True, 't': True, 'yes': True, 'y': True, 'false': False, 'f': False, 'no': False,
             'n': False, }

    if isinstance(value, bool):
        return value

    if not isinstance(value, str):
        raise ValueError('invalid literal for boolean. Not a string.')

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)

def python_baseline_parse(s):
    assert isinstance(s, str)
    # try to parse s as different types
    if s in ['', ]:
        return None
    try:
        return python_baseline_to_bool(s.strip())
    except:
        pass
    try:
        return int(s.strip())
    except:
        pass
    try:
        return float(s.strip())
    except:
        pass
    try:
        return json.loads(s.strip())
    except:
        pass
    # return as string, final option remaining...
    return s

def process_path_with_python_flights(input_path, year_lower, year_upper, dest_output_path):

    # handwritten pipeline (optimized)
    # this resembles the following pipeline to process a CSV file to a CSV file
    #     ctx.csv(input_pattern, sampling_mode=sm) \
    #         .withColumn("features", extract_feature_vector) \
    #         .map(fill_in_delays) \
    #         .filter(lambda x: year_lower <= x['year'] <= year_upper) \
    #         .tocsv(s3_output_path)
    tstart = time.time()
    rows = []
    num_input_rows = 0
    with open(input_path, 'r', encoding='latin1') as fp:
        reader = csv.DictReader(fp, delimiter=',', quotechar='"')
        for row in reader:
            row = dict(row)
            # parse dict to correct types
            row = {k: python_baseline_parse(v) for k, v in row.items()}

            num_input_rows += 1

            row["features"] = extract_feature_vector(row)
            row = fill_in_delays(row)

            if year_lower <= row['year'] <= year_upper:
                rows.append(row)

    # write out as CSV (escape each)
    if rows:

        # create parent folder first
        pathlib.Path(dest_output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(dest_output_path, 'w', newline='') as csvfile:
            # use same order here as Tuplex does, alternative would be something like
            # sorted([str(key) for key in rows[0].keys()])
            fieldnames = ['year', 'month', 'day', 'carrier', 'flightno', 'origin', 'dest', 'distance', 'dep_delay',
                          'arr_delay', 'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    if os.path.exists(dest_output_path):
        output_result = human_readable_size(os.path.getsize(dest_output_path))
    else:
        output_result = "skipped"

    num_output_rows = len(rows)

    duration = time.time() - tstart
    logging.info(f"Done in {duration:.2f}s, wrote output to {dest_output_path} ({output_result}, {num_output_rows} rows)")

    return {'input_path': input_path, 'output_path': dest_output_path, 'duration': duration,
            'num_output_rows': num_output_rows, 'num_input_rows': num_input_rows}


def run_with_python_baseline(args, **kwargs):
    startup_time = 0
    job_time = 0

    output_path = args.output_path
    input_pattern = args.input_pattern
    scratch_dir = args.scratch_dir

    if not output_path:
        raise ValueError('No output path specified')
    if not input_pattern:
        raise ValueError('No input_pattern specified')
    # if not scratch_dir:
    #     raise ValueError('No scratch directory specified')

    tstart = time.time()

    # Step 1: glob files (python only supports local mode (?) )
    input_paths = glob_paths(input_pattern)
    total_input_size = sum(map(lambda path: os.path.getsize(path), input_paths))
    logging.info(f"Found {len(input_paths)} input paths, total size: {human_readable_size(total_input_size)}")

    # Process each file now using handwritten pipeline
    total_output_rows = 0
    total_input_rows = 0
    path_stats = []
    for part_no, path in enumerate(input_paths):
        logging.info(f"Processing path {part_no+1}/{len(input_paths)}: {path} ({human_readable_size(os.path.getsize(path))})")
        ans = process_path_with_python_flights(path, kwargs['year_lower'], kwargs['year_upper'], os.path.join(output_path, "part_{:04d}.csv".format(part_no)))
        path_stats.append(ans)
        total_output_rows += ans['num_output_rows']
        total_input_rows += ans['num_input_rows']

    job_time = time.time() - tstart
    logging.info(f'total output rows: {total_output_rows}')
    stats = {"startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'python',
             'output_path': output_path,
             'input_path': input_pattern, 'scratch_path': scratch_dir, 'total_input_paths_size_in_bytes': total_input_size,
             'total_output_rows': total_output_rows, 'total_input_rows': total_input_rows, 'per_file_stats': path_stats}
    return stats

## Lithops


def lithops_flights_pipeline_per_file_with_custom_code(bucket_name, obj_key, out_bucket, out_suffix, storage, kwargs):
    # bucket name is local path
    # out_suffix is local path
    input_path = bucket_name
    output_path = out_suffix

    assert input_path
    assert output_path

    # use custom function (shared with python baseline)
    ans = process_path_with_python_flights(input_path, kwargs['year_lower'], kwargs['year_upper'],
                                           output_path)

    return ans

def run_with_lithops(args, **kwargs):
    tstart = time.time()
    import lithops
    version = lithops.__version__
    logging.info(f'Running with Lithops v{version}')
    startup_time = time.time() - tstart

    tstart = time.time()

    output_path = args.output_path
    input_pattern = args.input_pattern
    scratch_dir = args.scratch_dir

    if not output_path:
        raise ValueError('No output path specified')
    if not input_pattern:
        raise ValueError('No input_pattern specified')


    # configure local worker (use later cloud worker incl. S3)
    # link: https://lithops-cloud.github.io/docs/source/compute_config/localhost.html
    n_worker_processes = 1
    logging.info(f"Configuring local host executor with {n_worker_processes} processes.")
    # can set docker image as runtime
    fexec = lithops.LocalhostExecutor(worker_processes=n_worker_processes, runtime='python3')


    # Step 1: glob files (lithops does not support globbing)
    input_paths = glob_paths(input_pattern)
    total_input_size = sum(map(lambda path: os.path.getsize(path), input_paths))
    logging.info(f"Found {len(input_paths)} input paths, total size: {human_readable_size(total_input_size)}")

    args = []
    for part_no, path in enumerate(input_paths):
        output_suffix = os.path.join(output_path, "part_{:04d}.csv".format(part_no))
        args.append((path, None, None, output_suffix, kwargs))

    logging.info("Starting processing of {len(args)} units.")
    fexec.map(lithops_flights_pipeline_per_file_with_custom_code, args)

    logging.info("Retrieving result.")
    ret = fexec.get_result()

    # Accumulate stats from Lithops result.
    total_output_rows = 0
    total_input_rows = 0
    for ans in ret:
        total_output_rows += ans['num_output_rows']
        total_input_rows += ans['num_input_rows']

    job_time = time.time() - tstart
    logging.info(f'total output rows: {total_output_rows}')

    job_time = time.time() - tstart
    stats = {"startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'lithops',
             'output_path': output_path,
             'input_path': input_pattern, 'scratch_path': scratch_dir,
             'total_input_paths_size_in_bytes': total_input_size,
             'total_output_rows': total_output_rows, 'total_input_rows': total_input_rows,
             'lithops_version': str(version)}
    return stats

def flights_pipeline(ctx, year_lower, year_upper, input_pattern, s3_output_path, sm):
    ctx.csv(input_pattern, sampling_mode=sm) \
        .withColumn("features", extract_feature_vector) \
        .map(fill_in_delays) \
        .filter(lambda x: year_lower <= x['year'] <= year_upper) \
        .tocsv(s3_output_path)

# local worker version
def run_with_tuplex(args, **kwargs):

    LOCAL_WORKER_PATH='/home/leonhards/projects/tuplex-public/tuplex/cmake-build-release-w-cereal/dist/bin/tuplex-worker'

    output_path = args.output_path
    input_pattern = args.input_pattern
    scratch_dir = args.scratch_dir

    if not output_path:
        raise ValueError('No output path specified')
    if not input_pattern:
        raise ValueError('No input_pattern specified')
    if not scratch_dir:
        raise ValueError('No scratch directory specified')

    use_hyper_specialization = not args.no_hyper
    use_filter_promotion = not args.no_promo
    use_constant_folding = not args.no_cf

    strata_size = args.strata_size
    samples_per_strata = args.samples_per_strata


    # manipulate here to contrain granularity
    input_split_size = "2GB"
    num_processes = 8

    import tuplex

    # use following as debug pattern
    sm_map = {'A': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'C': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'D': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'E': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
              'F': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
              }

    sm = sm_map['D']  # ism_map.get(args.sampling_mode, None)
    sm = sm_map['B']

    if use_hyper_specialization:
        sm = sm_map['D']
    else:
        sm = sm_map['D']

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, output_path))
    print('    running in interpreter mode: {}'.format(args.python_mode))
    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    filter-promotion: {}'.format(use_filter_promotion))
    print('    null-value optimization: {}'.format(not args.no_nvo))
    print('    strata: {} per {}'.format(samples_per_strata, strata_size))
    # load data
    tstart = time.time()

    num_processes = 0

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable": False,
            "backend": "worker",
            "experimental.worker.numWorkers": num_processes,
            "experimental.worker.workerPath": LOCAL_WORKER_PATH,
            "aws.lambdaTimeout": 900,  # maximum allowed is 900s!
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'sample.maxDetectionMemory': '32MB',
            'sample.strataSize': strata_size,
            'sample.samplesPerStrata': samples_per_strata,
            "aws.scratchDir": scratch_dir,
            "scratchDir": scratch_dir,
            "autoUpcast": True,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.generateParser": False,  # not supported on lambda yet
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "optimizer.filterPromotion": use_filter_promotion,
            "optimizer.selectionPushdown": True,
            "useInterpreterOnly": args.python_mode,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}


    # if os.path.exists('tuplex_config.json'):
    #     with open('tuplex_config.json') as fp:
    #         conf = json.load(fp)

    conf['inputSplitSize'] = input_split_size
    # disable for now.
    conf["experimental.opportuneCompilation"] = False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    conf["inputSplitSize"] = input_split_size

    # config for single-threaded processing to avoid spilling (requires machine with enough memory)
    # co.set("tuplex.inputSplitSize", "20G");
    # co.set("tuplex.experimental.worker.workerBufferSize", "12G");
    conf["inputSplitSize"] = "20G"  # no splitting, process files as is with hyper & compare to single-threaded
    conf["experimental.worker.workerBufferSize"] = "12G" # no spilling

    tstart = time.time()

    ctx = tuplex.Context(conf)
    print('>>> Tuplex options: \n{}'.format(json.dumps(ctx.options())))

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    flights_pipeline(ctx, kwargs['year_lower'], kwargs['year_upper'], input_pattern, output_path, sm)

    ### END QUERY ###
    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    stats = {"benchmark": "flights", "startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'tuplex', 'output_path': output_path,
             'input_path': input_pattern, 'scratch_path': scratch_dir, 'options': ctx.options(), 'metrics': json.loads(m.as_json())}
    return stats


def run_with_tuplex_on_lambda(args):
    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_parallelism = args.lambda_parallelism
    logging.info(f"Using Lambda parallelism of {lambda_parallelism} max simultaneous active Lambdas.")

    lambda_price_per_gb_s = 0.0000166667 # x86 price
    lambda_size = max(128, min(args.lambda_size, 10240))
    lambda_price_per_h = lambda_size / 1000.0 * lambda_price_per_gb_s * 3600.0
    logging.info(f"Configuring Lambda to use {lambda_size} MB at most (${lambda_price_per_h:.2f}/h per lambda, max ${lambda_price_per_h * lambda_parallelism:.2f}/h for all configured lambdas).")

    output_path = args.output_path
    input_pattern = args.input_pattern
    scratch_dir = args.scratch_dir

    # use S3 paths (need to adjust later)
    output_path = S3_DEFAULT_OUTPUT_PATH
    input_pattern = S3_DEFAULT_INPUT_PATTERN
    aws_scratch_dir = S3_DEFAULT_SCRATCH_DIR


    # Check if s3 output path is given, if so use that one.
    if args.output_path.startswith('s3://'):
        output_path = args.output_path
    logging.info(f"Storing results in S3 output path: {output_path}")

    if not scratch_dir:
        raise ValueError('No scratch directory specified')

    use_hyper_specialization = not args.no_hyper
    use_filter_promotion = not args.no_promo
    use_constant_folding = not args.no_cf

    use_sparse_structs = True # this won't matter.
    use_generic_dicts = False # this won't matter. args.generic_dicts

    strata_size = args.strata_size
    samples_per_strata = args.samples_per_strata

    # manipulate here to constrain granularity
    input_split_size = "500GB" # as large as possible

    import tuplex

    # use following as debug pattern
    sm_map = {'A': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'C': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'D': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'E': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
              'F': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
              }

    sm = sm_map['D']  # ism_map.get(args.sampling_mode, None)
    sm = sm_map['B']

    if use_hyper_specialization:
        sm = sm_map['D']
    else:
        sm = sm_map['D']

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, output_path))
    print('    running in interpreter mode: {}'.format(args.python_mode))
    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    filter-promotion: {}'.format(use_filter_promotion))
    print('    null-value optimization: {}'.format(not args.no_nvo))
    print(f'    sparse-structs: {use_sparse_structs}')
    print(f'    generic-dicts: {use_generic_dicts}')
    print('    strata: {} per {}'.format(samples_per_strata, strata_size))
    print(f'    worker: {args.tuplex_worker_path}')
    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable": False,
            "backend": "lambda",
            "aws.lambdaTimeout": 900,  # maximum allowed is 900s!
            "aws.httpThreadCount": lambda_parallelism,
            "aws.maxConcurrency": lambda_parallelism,
            "aws.requesterPay": True,
            "aws.lambdaInvocationStrategy":"tree",
            "experimental.minimumSizeToSpecialize":0,
            "experimental.hyperspecialization": use_hyper_specialization,
            'sample.maxDetectionMemory': '32MB',
            'sample.strataSize': strata_size,
            'sample.samplesPerStrata': samples_per_strata,
            "aws.scratchDir": aws_scratch_dir,
            "autoUpcast": True,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.generateParser": False,  # not supported on lambda yet
            "optimizer.nullValueOptimization": True,
            "tuplex.experimental.useGenericDicts":use_generic_dicts,
            "tuplex.optimizer.sparsifyStructs":use_sparse_structs,
            # "resolveWithInterpreterOnly": False,
            "resolveWithInterpreterOnly": False, # use compiled slow path in addition.
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "optimizer.filterPromotion": use_filter_promotion,
            "optimizer.selectionPushdown": True,
            "optimizer.simplifyLargeStructs": False,
            "optimizer.simplifyLargeStructs.threshold": 20,
            "useInterpreterOnly": args.python_mode,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}


    # Special case: mode == python:
    if args.mode == "python":
        logging.info(f"LAMBDAs using Python interpreter only (mode={args.mode}).")
        conf["resolveWithInterpreterOnly"] = True
        conf["useInterpreterOnly"] = True
    else:
        logging.info(f"LAMBDAs using compiled paths and interpreter (mode={args.mode}).")
        conf["resolveWithInterpreterOnly"] = False
        conf["useInterpreterOnly"] = False

    # In hyper mode to avoid long LLVM IR optimization times, enable simplifyLargeStructs optimization. Deactivate it for global sparse structs,
    # as this mode specifically needs to measure how badly global structs affect everything.
    if use_hyper_specialization:
        conf["optimizer.simplifyLargeStructs"] = True
        # leave threshold as above.

    conf['inputSplitSize'] = input_split_size
    # disable for now.
    conf["experimental.opportuneCompilation"] = False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    conf["inputSplitSize"] = input_split_size

    # config for single-threaded processing to avoid spilling (requires machine with enough memory)
    # co.set("tuplex.inputSplitSize", "20G");
    # co.set("tuplex.experimental.worker.workerBufferSize", "12G");
    conf["inputSplitSize"] = "20G"  # no splitting, process files as is with hyper & compare to single-threaded

    tstart = time.time()

    ctx = tuplex.Context(conf)
    print('>>> Tuplex options: \n{}'.format(json.dumps(ctx.options())))

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    flights_pipeline(ctx, kwargs['year_lower'], kwargs['year_upper'], input_pattern, output_path, sm)

    ### END QUERY ###
    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    stats = {"benchmark":"flights", "startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'tuplex-on-lambda', 'output_path': output_path,
             'input_path': input_pattern, 'scratch_path': scratch_dir, 'options': ctx.options(), 'metrics': json.loads(m.as_json())}
    return stats

def setup_logging(log_path:Optional[str]) -> None:

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

    # add file handler to root logger
    if log_path:
        os.makedirs(pathlib.Path(log_path).parent, exist_ok=True)
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        handlers.append(handler)

        # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATE_FORMAT,
                        handlers=handlers)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flights hyper specialization query')
    # tuplex parameters
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-promo', dest='no_promo', action="store_true",
                        help="deactivate filter-promotion optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--python-mode', dest='python_mode', action="store_true",
                        help="process in pure python mode.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplex storage format for exceptions, no CSV/JSON format optimization',
                        action='store_true')
    parser.add_argument('--samples-per-strata', dest='samples_per_strata', default=10,
                        help='how many samples to use per strata')
    parser.add_argument('--strata-size', dest='strata_size', default=1024,
                        help='how many samples to use per strata')

    # experiment specific parameters
    parser.add_argument('--num-years', dest='num_years', action='store', choices=['auto'] + [str(year) for year in list(range(1, 2021-1987+2))], default='auto', help='if auto the range 2002-2005 will be used (equivalent to --num-years=4).')

    AVAILABLE_MODES = ['tuplex', 'python', 'lithops']

    # general args
    parser.add_argument('--m', '--mode', dest='mode', choices=AVAILABLE_MODES, default='tuplex', help='select whether to run benchmark using python baseline or tuplex')
    parser.add_argument('--input-pattern', default=None, dest='input_pattern', help='input files to read into flights pipeline')
    parser.add_argument('--output-path', default=None, dest='output_path', help='where to store result of pipeline')
    parser.add_argument('--scratch-dir', default=None, dest='scratch_dir', help='where to store intermediate results')
    parser.add_argument('--log-path', default=None, dest='log_path', help='specify optional path where to store experiment log results.')
    parser.add_argument('--result-path', default='results.ndjson', help='new-line delimited JSON formatted result file')
    parser.add_argument('--tuplex-worker-path', default=None, dest="tuplex_worker_path", help="specify worker path when executing in local mode.")
    # AWS Lambda specific experiment settings.
    parser.add_argument('--lambda', dest="with_lambda", action="store_true")
    parser.add_argument('--lambda-parallelism', dest="lambda_parallelism", type=int, default=100)
    parser.add_argument('--lambda-size', dest="lambda_size", type=int, default=1536)
    args = parser.parse_args()

    # set up logging, by default always render to console. If log path is present, store file as well
    setup_logging(args.log_path)
    logging.info("Running Flights query benchmark for Tuplex/Viton.")
    if args.log_path is not None:
        logging.info("Saving logs to {}".format(args.log_path))

    # manipulate inout pattern depending on files
    # here are the default values (auto)
    year_lower = 2002
    year_upper = 2005
    if args.num_years != 'auto':
        num_years = int(args.num_years)
        years = make_year_range(num_years)
        logging.info('-- Running with filter over years: {}'.format(', '.join([str(year) for year in years])))
        year_lower = min(years)
        year_upper = max(years)
    logging.info(f"-- filter: lambda x: {year_lower} <= x['year'] <= {year_upper}")
    kwargs = {'year_lower': year_lower, 'year_upper':year_upper}


    # Run different modes.
    if args.with_lambda:
        logging.info("Running tuplex on AWS lambdas.")
        ans = run_with_tuplex_on_lambda(args)

        # Load .json file from job/ folder.
        # The current one is the latest written one.
        JOB_STATS_FILE = sorted(list(glob.glob("job/*.json")), key=os.path.getctime)[-1]
        logging.info(f"Found existing job stats file {JOB_STATS_FILE}.")
        if os.path.exists(JOB_STATS_FILE):
            with open(JOB_STATS_FILE, 'r') as fp:
                ans['detailed_job_stats'] = json.load(fp)
        else:
            logging.error(f"Could not find worker app stats ({JOB_STATS_FILE})")

    elif args.mode == 'tuplex':
        ans = run_with_tuplex(args, **kwargs)

        # load worker_app_job.json and append to results
        JOB_STATS_FILE='worker_app_job.json'
        if os.path.exists(JOB_STATS_FILE):
            with open(JOB_STATS_FILE, 'r') as fp:
                ans['detailed_job_stats'] = json.load(fp)
        else:
            logging.error(f"Could not find worker app stats ({JOB_STATS_FILE})")
    elif args.mode == 'python':
        ans = run_with_python_baseline(args, **kwargs)
    elif args.mode == 'lithops':
        ans = run_with_lithops(args, **kwargs)
    else:
        raise NotImplementedError(f'Mode {args.mode} not yet implemented')

    logging.info(f"pipeline in mode {args.mode} took {ans['job_time_in_s']:.2f} seconds")
    logging.info(f"Storing results in {args.result_path} via append")
    os.makedirs(pathlib.Path(args.result_path).parent, exist_ok=True)
    with open(args.result_path, 'a') as f:
        json.dump(ans, f, sort_keys=True)
        f.write('\n')
    logging.info("Done.")
    os._exit(0)