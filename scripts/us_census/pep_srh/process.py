# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
This module creates CSV files used for importing data into DC.

Below are list of files processed -
County
    1990 - 2000     Processed As Is
    2000 - 2010     Processed As Is
    2010 - 2020     Processed As Is

State
    1980 - 1990     Processed As Is
    1990 - 2000     Processed As Is
    2000 - 2010     Aggregated from County to state
                    (data matches with State Level files available)
    2010 - 2020     Aggregated from County to state
                    (data matches with State Level files available)

National
    1980 - 1990     Aggregted from State to National Level
    1990 - 2000     Aggregted from State to National Level
    2000 - 2010     Aggregted from State to National Level
    2010 - 2020     Aggregted from State to National Level

Also SV aggregation are produced while processing above files.
E.g., Count_Person_White_HispanicOrLatino is calulated by addding
Count_Person_Male_WhiteHispanicOrLatino and
Count_Person_Male_WhiteHispanicOrLatino

Before running this module, run download.sh script, it downloads required
input files, created necessary folders for processing.

Folder information
download_files - downloaded files are placed here
process_files - intermediate processed files are placed in this folder.
output_files - output files (mcf, tmcf and csv are written here)
'''

import os
import pandas as pd

from constants import DOWNLOAD_DIR, PROCESS_AS_IS_DIR, PROCESS_AGG_DIR
from constants import OUTPUT_DIR, stat_var_col_mapping, working_directories

_CODEDIR = os.path.dirname(os.path.realpath(__file__))


def process_national_files_1980_1990():
    '''
    Process for National Level data, 1980 - 1990
    Uses State Level file as input
    '''
    # Section 1 - Writing As Is Data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1980_1990/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1980_1990/national/'
    output_file_name = 'national_1980_1990.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    input_file_path = _CODEDIR + PROCESS_AGG_DIR + '1980_1990/state/'
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '1980_1990/national/'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)


def process_national_files_1990_2000():
    '''
    Process for National Level data, 1990 - 2000
    Uses State Level file as input
    '''
    # Section 1 - Writing As Is Data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1990_2000/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1990_2000/national/'
    output_file_name = 'national_1990_2000.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    input_file_path = _CODEDIR + PROCESS_AGG_DIR + '1990_2000/state/'
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '1990_2000/national/'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)


def process_national_files_2000_2010():
    '''
    Process for National Level data, 2000 - 2010
    Uses State Level file as input
    '''
    # Section 1 - Writing As Is Data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2000_2010/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2000_2010/national/'
    output_file_name = 'national_2000_2010.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2000_2010/national/'
    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()
    df1['NH'] = (df['NH_MALE'] + df['NH_FEMALE']).copy()
    df1['NHWA'] = (df['NHWA_MALE'] + df['NHWA_FEMALE']).copy()
    df1['NHBA'] = (df['NHBA_MALE'] + df['NHBA_FEMALE']).copy()
    df1['NHIA'] = (df['NHIA_MALE'] + df['NHIA_FEMALE']).copy()
    df1['NHAA'] = (df['NHAA_MALE'] + df['NHAA_FEMALE']).copy()
    df1['NHNA'] = (df['NHNA_MALE'] + df['NHNA_FEMALE']).copy()
    df1['NHTOM'] = (df['NHTOM_MALE'] + df['NHTOM_FEMALE']).copy()
    df1['H'] = (df['H_MALE'] + df['H_FEMALE']).copy()
    df1['HWA'] = (df['HWA_MALE'] + df['HWA_FEMALE']).copy()
    df1['HBA'] = (df['HBA_MALE'] + df['HBA_FEMALE']).copy()
    df1['HIA'] = (df['HIA_MALE'] + df['HIA_FEMALE']).copy()
    df1['HAA'] = (df['HAA_MALE'] + df['HAA_FEMALE']).copy()
    df1['HNA'] = (df['HNA_MALE'] + df['HNA_FEMALE']).copy()
    df1['HTOM'] = (df['HTOM_MALE'] + df['HTOM_FEMALE']).copy()
    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_national_files_2010_2020():
    '''
    Process national files from 2010 - 2020
    Uses state level file as input
    '''
    # Section 1 - Writing As Is Data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2010_2020/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2010_2020/national/'
    output_file_name = 'national_2010_2020.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    input_file_path = _CODEDIR + PROCESS_AGG_DIR + '2010_2020/state/'
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2010_2020/national/'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = 'country/USA'
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)


def process_national_files():
    '''
    Process national files from 1980 - 2020
    '''
    process_national_files_1980_1990()
    process_national_files_1990_2000()
    process_national_files_2000_2010()
    process_national_files_2010_2020()


def process_state_files_1980_1990(download_dir):
    '''
    Process state files from 1980 - 1990
    Age is present in bracket for 5 years, these are added up
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + download_dir + '1980_1990/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1980_1990/state/'
    output_file_name = 'state_1980_1990.csv'
    output_temp_file_name = 'state_1980_1990_temp.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    column_names = [
        "LOCATION", "YEAR_TEMP", "RACE_ORIGIN", "SEX", "A01", "A02", "A03",
        "A04", "A05", "A06", "A07", "A08", "A09", "A10", "A11", "A12", "A13",
        "A14", "A15", "A16", "A17", "A18"
    ]

    column_specification = [(0, 2), (2, 3), (3, 4), (4, 5), (5, 12), (12, 19),
                            (19, 26), (26, 33), (33, 40), (40, 47), (47, 54),
                            (54, 61), (61, 68), (68, 75), (75, 82), (82, 89),
                            (89, 96), (96, 103), (103, 110), (110, 117),
                            (117, 124), (124, 131)]

    df = pd.read_fwf(input_file_path + files_list[0],
                     names=column_names,
                     colspecs=column_specification)

    df["ALL_AGE"] = (df["A01"] + df["A02"] + df["A03"] + df["A04"] + df["A05"] +
                     df["A06"] + df["A07"] + df["A08"] + df["A09"] + df["A10"] +
                     df["A11"] + df["A12"] + df["A13"] + df["A14"] + df["A15"] +
                     df["A16"] + df["A17"] + df["A18"])

    df.drop([
        "A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09", "A10",
        "A11", "A12", "A13", "A14", "A15", "A16", "A17", "A18"
    ],
            axis=1,
            inplace=True)

    df = df.pivot(index=['LOCATION', 'YEAR_TEMP'],
                  columns=['RACE_ORIGIN', 'SEX'],
                  values='ALL_AGE').reset_index()

    df.to_csv(output_file_path + output_temp_file_name,
              header=True,
              index=False)

    df = pd.read_csv(output_file_path + output_temp_file_name)
    # On pivoting one new row is introduced with NaN - hence dropping
    df = df.dropna()
    # Some of the columns are read as Float, hence converting them to Int
    float_col = df.select_dtypes(include=['float64'])
    for col in float_col.columns.values:
        df[col] = df[col].astype('int64')

    df.insert(0, 'YEAR', 1980 + df['YEAR_TEMP'], True)
    df['LOCATION'] = 'geoId/' + (df['LOCATION'].map(str)).str.zfill(2)
    df.drop(["YEAR_TEMP"], axis=1, inplace=True)
    df.columns = [
        'YEAR', 'LOCATION', 'NH-W-M', 'NH-W-F', 'NH-B-M', 'NH-B-F', 'NH-AI-M',
        'NH-AI-F', 'NH-API-M', 'NH-API-F', 'H-W-M', 'H-W-F', 'H-B-M', 'H-B-F',
        'H-AI-M', 'H-AI-F', 'H-API-M', 'H-API-F'
    ]

    df.to_csv(output_file_path + output_file_name, index=False)

    if os.path.exists(output_file_path + output_temp_file_name):
        os.remove(output_file_path + output_temp_file_name)

    # Section 2 - Writing Agg data
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '1980_1990/state/'

    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()

    df1['NH'] = (df['NH-W-M'] + df['NH-W-F'] + df['NH-B-M'] + df['NH-B-F'] +
                 df['NH-AI-M'] + df['NH-AI-F'] + df['NH-API-M'] +
                 df['NH-API-F']).copy()
    df1['NH-W'] = (df['NH-W-M'] + df['NH-W-F']).copy()
    df1['NH-B'] = (df['NH-B-M'] + df['NH-B-F']).copy()
    df1['NH-AI'] = (df['NH-AI-M'] + df['NH-AI-F']).copy()
    df1['NH-API'] = (df['NH-API-M'] + df['NH-API-F']).copy()

    df1['H'] = (df['H-W-M'] + df['H-W-F'] + df['H-B-M'] + df['H-B-F'] +
                df['H-AI-M'] + df['H-AI-F'] + df['H-API-M'] +
                df['H-API-F']).copy()
    df1['H-W'] = (df['H-W-M'] + df['H-W-F']).copy()
    df1['H-B'] = (df['H-B-M'] + df['H-B-F']).copy()
    df1['H-AI'] = (df['H-AI-M'] + df['H-AI-F']).copy()
    df1['H-API'] = (df['H-API-M'] + df['H-API-F']).copy()

    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_state_files_1990_2000(download_dir):
    '''
    Process state files from 1990 - 2000
    There are multiple files present for this duration.
    All of them are in same format, they are all read and written to temp
    file as first temp. Post which processing is done in this single file.
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + download_dir + '1990_2000/state/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1990_2000/state/'
    output_file_name = 'state_1990_2000.csv'
    output_temp_file_name = 'state_1990_2000_temp.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    column_names = [
        "YEAR", "LOCATION", "AGE", "NH-W-M", "NH-W-F", "NH-B-M", "NH-B-F",
        "NH-AIAN-M", "NH-AIAN-F", "NH-API-M", "NH-API-F", "H-W-M", "H-W-F",
        "H-B-M", "H-B-F", "H-AIAN-M", "H-AIAN-F", "H-API-M", "H-API-F"
    ]

    column_specification = [(0, 4), (5, 7), (8, 11), (12, 19), (19, 26),
                            (27, 34), (34, 41), (42, 49), (49, 56), (57, 64),
                            (64, 71), (72, 79), (79, 86), (87, 94), (94, 101),
                            (102, 109), (109, 116), (117, 124), (124, 131)]

    for file in files_list:
        temp_file_df = pd.read_fwf(input_file_path + file,
                                   names=column_names,
                                   colspecs=column_specification,
                                   skiprows=16)
        if file == files_list[0]:
            temp_file_df.to_csv(output_file_path + output_temp_file_name,
                                header=True,
                                index=False)
        else:
            temp_file_df.to_csv(output_file_path + output_temp_file_name,
                                header=False,
                                index=False,
                                mode='a')

    df = pd.read_csv(output_file_path + output_temp_file_name)
    df.drop(["AGE"], axis=1, inplace=True)
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df['LOCATION'] = 'geoId/' + (df['LOCATION'].map(str)).str.zfill(2)
    df.to_csv(output_file_path + output_file_name, index=False)

    # Deleteing temp (concatenated file)
    if os.path.exists(output_file_path + output_temp_file_name):
        os.remove(output_file_path + output_temp_file_name)

    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '1990_2000/state/'

    # Section 2 - Writing Agg data
    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()

    df1['NH'] = (df['NH-W-M'] + df['NH-W-F'] + df['NH-B-M'] + df['NH-B-F'] +
                 df['NH-AIAN-M'] + df['NH-AIAN-F'] + df['NH-API-M'] +
                 df['NH-API-F']).copy()
    df1['NH-W'] = (df['NH-W-M'] + df['NH-W-F']).copy()
    df1['NH-B'] = (df['NH-B-M'] + df['NH-B-F']).copy()
    df1['NH-AIAN'] = (df['NH-AIAN-M'] + df['NH-AIAN-F']).copy()
    df1['NH-API'] = (df['NH-API-M'] + df['NH-API-F']).copy()

    df1['H'] = (df['H-W-M'] + df['H-W-F'] + df['H-B-M'] + df['H-B-F'] +
                df['H-AIAN-M'] + df['H-AIAN-F'] + df['H-API-M'] +
                df['H-API-F']).copy()
    df1['H-W'] = (df['H-W-M'] + df['H-W-F']).copy()
    df1['H-B'] = (df['H-B-M'] + df['H-B-F']).copy()
    df1['H-AIAN'] = (df['H-AIAN-M'] + df['H-AIAN-F']).copy()
    df1['H-API'] = (df['H-API-M'] + df['H-API-F']).copy()

    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_state_files_2000_2010():
    '''
    Process state files from 2000 - 2010
    Uses County data to add up to State Level (this matches with State Level
    file available)
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2000_2010/county/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2000_2010/state/'
    output_file_name = 'state_2000_2010.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = (df['LOCATION'].map(str)).str[:8]
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, header=True, index=False)

    # Section 2 - Writing Agg data
    input_file_path = _CODEDIR + PROCESS_AGG_DIR + '2000_2010/county/'
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2000_2010/state/'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = (df['LOCATION'].map(str)).str[:8]
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)


def process_state_files_2010_2020():
    '''
    Process state files from 2010 - 2020
    Uses County data to add up to State Level (this matches with State Level
    file available)
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2010_2020/county/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2010_2020/state/'
    output_file_name = 'state_2010_2020.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = (df['LOCATION'].map(str)).str[:8]
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    input_file_path = _CODEDIR + PROCESS_AGG_DIR + '2010_2020/county/'
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2010_2020/state/'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0])
    df['LOCATION'] = (df['LOCATION'].map(str)).str[:8]
    df = df.groupby(['YEAR', 'LOCATION']).agg('sum').reset_index()
    df.to_csv(output_file_path + output_file_name, index=False)


def process_state_files(download_dir):
    '''
    Process state files from 1980 - 2020
    '''
    process_state_files_1980_1990(download_dir)
    process_state_files_1990_2000(download_dir)
    process_state_files_2000_2010()
    process_state_files_2010_2020()


def process_county_files_1990_2000(download_dir):
    '''
    Process County files 1990 2000
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + download_dir + '1990_2000/county/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '1990_2000/county/'
    output_file_name = 'county_1990_2000.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    column_names = [
        "YEAR",
        "LOCATION",
        "NH-W",
        "NH-B",
        "NH-AIAN",
        "NH-API",
        "H-W",
        "H-B",
        "H-AIAN",
        "H-API",
    ]

    column_specification = [(0, 4), (5, 10), (10, 19), (19, 28), (28, 37),
                            (37, 46), (46, 55), (55, 64), (64, 73), (73, 82)]

    df = pd.read_fwf(input_file_path + files_list[0],
                     names=column_names,
                     colspecs=column_specification,
                     header=None,
                     skiprows=18)

    df['LOCATION'] = 'geoId/' + (df['LOCATION'].map(str)).str.zfill(5)
    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '1990_2000/county/'

    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()
    df1['NH'] = (df['NH-W'] + df['NH-B'] + df['NH-AIAN'] + df['NH-API']).copy()
    df1['H'] = (df['H-W'] + df['H-B'] + df['H-AIAN'] + df['H-API']).copy()

    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_county_files_2000_2010(download_dir):
    '''
    Process County files 2000 2010
    '''
    # Section 1 - Writing As Is data
    input_file_path = _CODEDIR + download_dir + '2000_2010/county/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2000_2010/county/'
    output_file_name = 'county_2000_2010.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    for file in files_list:
        df = pd.read_csv(input_file_path + file, encoding='ISO-8859-1')
        df = df.query('AGEGRP == 99  & YEAR not in [1, 12, 13]').copy()
        df['YEAR'] = 2000 - 2 + df['YEAR']
        df.insert(7, 'LOCATION', '', True)
        df['LOCATION'] = 'geoId/' + (df['STATE'].map(str)).str.zfill(2) + (
            df['COUNTY'].map(str)).str.zfill(3)
        df.drop(['SUMLEV', 'STATE', 'COUNTY', 'STNAME', 'CTYNAME', 'AGEGRP'],
                axis=1,
                inplace=True)

        if file == files_list[0]:
            df.to_csv(output_file_path + output_file_name,
                      header=True,
                      index=False)
        else:
            df.to_csv(output_file_path + output_file_name,
                      header=False,
                      index=False,
                      mode='a')

    df = pd.read_csv(output_file_path + output_file_name)

    # Section 2 - Writing Agg data
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2000_2010/county/'
    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()

    df1['NH'] = (df['NH_MALE'] + df['NH_FEMALE']).copy()
    df1['NHWA'] = (df['NHWA_MALE'] + df['NHWA_FEMALE']).copy()
    df1['NHBA'] = (df['NHBA_MALE'] + df['NHBA_FEMALE']).copy()
    df1['NHIA'] = (df['NHIA_MALE'] + df['NHIA_FEMALE']).copy()
    df1['NHAA'] = (df['NHAA_MALE'] + df['NHAA_FEMALE']).copy()
    df1['NHNA'] = (df['NHNA_MALE'] + df['NHNA_FEMALE']).copy()
    df1['NHTOM'] = (df['NHTOM_MALE'] + df['NHTOM_FEMALE']).copy()

    df1['H'] = (df['H_MALE'] + df['H_FEMALE']).copy()
    df1['HWA'] = (df['HWA_MALE'] + df['HWA_FEMALE']).copy()
    df1['HBA'] = (df['HBA_MALE'] + df['HBA_FEMALE']).copy()
    df1['HIA'] = (df['HIA_MALE'] + df['HIA_FEMALE']).copy()
    df1['HAA'] = (df['HAA_MALE'] + df['HAA_FEMALE']).copy()
    df1['HNA'] = (df['HNA_MALE'] + df['HNA_FEMALE']).copy()
    df1['HTOM'] = (df['HTOM_MALE'] + df['HTOM_FEMALE']).copy()

    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_county_files_2010_2020(download_dir):
    '''
    Process County files 2010 2020
    '''
    input_file_path = _CODEDIR + download_dir + '2010_2020/county/'
    output_file_path = _CODEDIR + PROCESS_AS_IS_DIR + '2010_2020/county/'
    output_file_name = 'county_2010_2020.csv'
    files_list = os.listdir(input_file_path)
    files_list.sort()

    df = pd.read_csv(input_file_path + files_list[0],
                     encoding='ISO-8859-1',
                     low_memory=False)
    # filter by agegrp = 0 (0 = sum of all age group added)
    # filter years 3 - 13 (1, 2 - is base estimate and not for month July)
    df = df.query("AGEGRP == 0 & YEAR not in [1, 2]").copy()

    # convert year code to year
    # Year code starting from 3 for Year 2010
    df['YEAR'] = df['YEAR'] + 2010 - 3

    # add fips code for location
    df.insert(6, 'LOCATION', 'geoId/', True)
    df['LOCATION'] = 'geoId/' + (df['STATE'].map(str)).str.zfill(2) + (
        df['COUNTY'].map(str)).str.zfill(3)

    # drop not reuqire columns
    df.drop(['SUMLEV', 'STATE', 'COUNTY', 'STNAME', 'CTYNAME', 'AGEGRP'],
            axis=1,
            inplace=True)

    df.to_csv(output_file_path + output_file_name, index=False)

    # Section 2 - Writing Agg data
    df = pd.read_csv(output_file_path + output_file_name)
    output_file_path = _CODEDIR + PROCESS_AGG_DIR + '2010_2020/county/'
    df1 = pd.DataFrame()
    df1['YEAR'] = df['YEAR'].copy()
    df1['LOCATION'] = df['LOCATION'].copy()

    df1['NH'] = (df['NH_MALE'] + df['NH_FEMALE']).copy()
    df1['NHWA'] = (df['NHWA_MALE'] + df['NHWA_FEMALE']).copy()
    df1['NHBA'] = (df['NHBA_MALE'] + df['NHBA_FEMALE']).copy()
    df1['NHIA'] = (df['NHIA_MALE'] + df['NHIA_FEMALE']).copy()
    df1['NHAA'] = (df['NHAA_MALE'] + df['NHAA_FEMALE']).copy()
    df1['NHNA'] = (df['NHNA_MALE'] + df['NHNA_FEMALE']).copy()
    df1['NHTOM'] = (df['NHTOM_MALE'] + df['NHTOM_FEMALE']).copy()
    df1['NHWAC'] = (df['NHWAC_MALE'] + df['NHWAC_FEMALE']).copy()
    df1['NHBAC'] = (df['NHBAC_MALE'] + df['NHBAC_FEMALE']).copy()
    df1['NHIAC'] = (df['NHIAC_MALE'] + df['NHIAC_FEMALE']).copy()
    df1['NHAAC'] = (df['NHAAC_MALE'] + df['NHAAC_FEMALE']).copy()
    df1['NHNAC'] = (df['NHNAC_MALE'] + df['NHNAC_FEMALE']).copy()

    df1['H'] = (df['H_MALE'] + df['H_FEMALE']).copy()
    df1['HWA'] = (df['HWA_MALE'] + df['HWA_FEMALE']).copy()
    df1['HBA'] = (df['HBA_MALE'] + df['HBA_FEMALE']).copy()
    df1['HIA'] = (df['HIA_MALE'] + df['HIA_FEMALE']).copy()
    df1['HAA'] = (df['HAA_MALE'] + df['HAA_FEMALE']).copy()
    df1['HNA'] = (df['HNA_MALE'] + df['HNA_FEMALE']).copy()
    df1['HTOM'] = (df['HTOM_MALE'] + df['HTOM_FEMALE']).copy()
    df1['HWAC'] = (df['HWAC_MALE'] + df['HWAC_FEMALE']).copy()
    df1['HBAC'] = (df['HBAC_MALE'] + df['HBAC_FEMALE']).copy()
    df1['HIAC'] = (df['HIAC_MALE'] + df['HIAC_FEMALE']).copy()
    df1['HAAC'] = (df['HAAC_MALE'] + df['HAAC_FEMALE']).copy()
    df1['HNAC'] = (df['HNAC_MALE'] + df['HNAC_FEMALE']).copy()

    df1.to_csv(output_file_path + output_file_name, header=True, index=False)


def process_county_files(download_dir):
    '''
    Process county files from 1990 - 2020
    '''
    process_county_files_1990_2000(download_dir)
    process_county_files_2000_2010(download_dir)
    process_county_files_2010_2020(download_dir)


def consolidate_national_files():
    '''
    Consolidating all national level files into single file
    At the same time only carrying data which is relevant to SV by SRH
    This funtion aggregates both as-is and agg data files
    '''
    national_as_is_files = [
        _CODEDIR + PROCESS_AS_IS_DIR +
        '1980_1990/national/national_1980_1990.csv', _CODEDIR +
        PROCESS_AS_IS_DIR + '1990_2000/national/national_1990_2000.csv',
        _CODEDIR + PROCESS_AS_IS_DIR +
        '2000_2010/national/national_2000_2010.csv', _CODEDIR +
        PROCESS_AS_IS_DIR + '2010_2020/national/national_2010_2020.csv'
    ]

    for file in national_as_is_files:
        df = pd.read_csv(file)

        if file == national_as_is_files[2]:
            df.drop([
                'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE',
                'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE',
                'AA_FEMALE', 'NA_MALE', 'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE'
            ],
                    axis=1,
                    inplace=True)

        if file == national_as_is_files[3]:
            df.drop([
                'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE',
                'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE',
                'AA_FEMALE', 'NA_MALE', 'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE',
                'WAC_MALE', 'WAC_FEMALE', 'BAC_MALE', 'BAC_FEMALE', 'IAC_MALE',
                'IAC_FEMALE', 'AAC_MALE', 'AAC_FEMALE', 'NAC_MALE', 'NAC_FEMALE'
            ],
                    axis=1,
                    inplace=True)

        df = df.melt(id_vars=['YEAR', 'LOCATION'],
                     var_name='SV',
                     value_name='OBSERVATION')
        df.replace({"SV": stat_var_col_mapping}, inplace=True)
        df["SV"] = 'dcid:' + df["SV"]

        df.insert(3, 'MEASUREMENT_METHOD', 'dcs:dcAggregate/CensusPEPSurvey',
                  True)

        if file == national_as_is_files[0]:
            df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'national_consolidated_temp.csv',
                      header=True,
                      index=False)
        else:
            df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'national_consolidated_temp.csv',
                      header=False,
                      index=False,
                      mode='a')

        df = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                         'national_consolidated_temp.csv')

    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
              'national_consolidated_as_is_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AS_IS_DIR \
        + 'national_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AS_IS_DIR +
                  'national_consolidated_temp.csv')

    # Aggregate file processing
    national_agg_files = [
        _CODEDIR + PROCESS_AGG_DIR +
        '1980_1990/national/national_1980_1990.csv', _CODEDIR +
        PROCESS_AGG_DIR + '1990_2000/national/national_1990_2000.csv',
        _CODEDIR + PROCESS_AGG_DIR +
        '2000_2010/national/national_2000_2010.csv',
        _CODEDIR + PROCESS_AGG_DIR + '2010_2020/national/national_2010_2020.csv'
    ]

    for file in national_agg_files:
        df = pd.read_csv(file)
        df = df.melt(id_vars=['YEAR', 'LOCATION'],
                     var_name='SV',
                     value_name='OBSERVATION')
        df.replace({"SV": stat_var_col_mapping}, inplace=True)
        df["SV"] = 'dcid:' + df["SV"]

        if file == national_agg_files[0]:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'national_consolidated_temp.csv',
                      header=True,
                      index=False)
        else:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'national_consolidated_temp.csv',
                      header=False,
                      index=False,
                      mode='a')

    df = pd.read_csv(
        _CODEDIR + PROCESS_AGG_DIR + 'national_consolidated_temp.csv',)
    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:dcAggregate/CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
              'national_consolidated_agg_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AGG_DIR \
        + 'national_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AGG_DIR + 'national_consolidated_temp.csv')


def consolidate_state_files():
    '''
    Consolidating all state level files into single file
    At the same time only carrying data which is relevant to SV by SRH
    This funtion consolidtes both as-is and agg inddividual state files
    '''
    ip_file = _CODEDIR + PROCESS_AS_IS_DIR \
        + '1980_1990/state/state_1980_1990.csv'

    df = pd.read_csv(ip_file)
    df = df.melt(id_vars=['YEAR', 'LOCATION'],
                 var_name='SV',
                 value_name='OBSERVATION')
    df.replace({"SV": stat_var_col_mapping}, inplace=True)
    df["SV"] = 'dcid:' + df["SV"]
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR + 'state_consolidated_temp.csv',
              header=True,
              index=False)

    ip_file = _CODEDIR + PROCESS_AS_IS_DIR \
        + '1990_2000/state/state_1990_2000.csv'

    df = pd.read_csv(ip_file)
    df = df.melt(id_vars=['YEAR', 'LOCATION'],
                 var_name='SV',
                 value_name='OBSERVATION')
    df.replace({"SV": stat_var_col_mapping}, inplace=True)
    df["SV"] = 'dcid:' + df["SV"]
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR + 'state_consolidated_temp.csv',
              header=False,
              index=False,
              mode='a')

    ip_file = _CODEDIR + PROCESS_AS_IS_DIR \
        + '2000_2010/state/state_2000_2010.csv'
    df = pd.read_csv(ip_file)
    df.drop([
        'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE', 'BA_MALE',
        'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE', 'AA_FEMALE', 'NA_MALE',
        'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE'
    ],
            axis=1,
            inplace=True)
    df = df.melt(id_vars=['YEAR', 'LOCATION'],
                 var_name='SV',
                 value_name='OBSERVATION')
    df.replace({"SV": stat_var_col_mapping}, inplace=True)
    df["SV"] = 'dcid:' + df["SV"]
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR + 'state_consolidated_temp.csv',
              header=False,
              index=False,
              mode='a')

    df = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                     '2010_2020/state/state_2010_2020.csv')
    df.drop([
        'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE', 'BA_MALE',
        'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE', 'AA_FEMALE', 'NA_MALE',
        'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE', 'WAC_MALE', 'WAC_FEMALE',
        'BAC_MALE', 'BAC_FEMALE', 'IAC_MALE', 'IAC_FEMALE', 'AAC_MALE',
        'AAC_FEMALE', 'NAC_MALE', 'NAC_FEMALE'
    ],
            axis=1,
            inplace=True)
    df = df.melt(id_vars=['YEAR', 'LOCATION'],
                 var_name='SV',
                 value_name='OBSERVATION')
    df.replace({"SV": stat_var_col_mapping}, inplace=True)
    df["SV"] = 'dcid:' + df["SV"]
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR + 'state_consolidated_temp.csv',
              header=False,
              index=False,
              mode='a')

    df = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                     'state_consolidated_temp.csv')
    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
              'state_consolidated_as_is_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AS_IS_DIR \
        + 'state_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AS_IS_DIR + 'state_consolidated_temp.csv')

    # Agg file processing
    state_agg_files = [
        _CODEDIR + PROCESS_AGG_DIR + '1980_1990/state/state_1980_1990.csv',
        _CODEDIR + PROCESS_AGG_DIR + '1990_2000/state/state_1990_2000.csv',
        _CODEDIR + PROCESS_AGG_DIR + '2000_2010/state/state_2000_2010.csv',
        _CODEDIR + PROCESS_AGG_DIR + '2010_2020/state/state_2010_2020.csv'
    ]

    for file in state_agg_files:
        df = pd.read_csv(file)
        df = df.melt(id_vars=['YEAR', 'LOCATION'],
                     var_name='SV',
                     value_name='OBSERVATION')
        df.replace({"SV": stat_var_col_mapping}, inplace=True)
        df["SV"] = 'dcid:' + df["SV"]

        if file == state_agg_files[0]:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'state_consolidated_temp.csv',
                      header=True,
                      index=False)
        else:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'state_consolidated_temp.csv',
                      header=False,
                      index=False,
                      mode='a')

    df = pd.read_csv(_CODEDIR + PROCESS_AGG_DIR + 'state_consolidated_temp.csv')
    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:dcAggregate/CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AGG_DIR + 'state_consolidated_agg_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AGG_DIR \
    + 'state_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AGG_DIR + 'state_consolidated_temp.csv')


def consolidate_county_files():
    '''
    Consolidating all county level files into single file
    At the same time only carrying data which is relevant to SV by SRH
    This funtion consolidtes both as-is and agg inddividual state files
    '''
    input_file1 = _CODEDIR + PROCESS_AS_IS_DIR \
        + '2000_2010/county/county_2000_2010.csv'
    input_file2 = _CODEDIR + PROCESS_AS_IS_DIR \
        + '2010_2020/county/county_2010_2020.csv'

    county_file = []
    county_file.append(input_file1)
    county_file.append(input_file2)

    for file in county_file:
        df = pd.read_csv(file)

        if file == county_file[0]:
            df.drop([
                'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE',
                'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE',
                'AA_FEMALE', 'NA_MALE', 'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE'
            ],
                    axis=1,
                    inplace=True)
            df = df.melt(id_vars=['YEAR', 'LOCATION'],
                         var_name='SV',
                         value_name='OBSERVATION')
            df.replace({"SV": stat_var_col_mapping}, inplace=True)
            df["SV"] = 'dcid:' + df["SV"]
            df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
            df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'county_consolidated_temp.csv',
                      header=True,
                      index=False)
        else:
            df.drop([
                'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE',
                'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE',
                'AA_FEMALE', 'NA_MALE', 'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE',
                'WAC_MALE', 'WAC_FEMALE', 'BAC_MALE', 'BAC_FEMALE', 'IAC_MALE',
                'IAC_FEMALE', 'AAC_MALE', 'AAC_FEMALE', 'NAC_MALE', 'NAC_FEMALE'
            ],
                    axis=1,
                    inplace=True)
            df = df.melt(id_vars=['YEAR', 'LOCATION'],
                         var_name='SV',
                         value_name='OBSERVATION')
            df.replace({"SV": stat_var_col_mapping}, inplace=True)
            df["SV"] = 'dcid:' + df["SV"]
            df.insert(3, 'MEASUREMENT_METHOD', 'dcs:CensusPEPSurvey', True)
            df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'county_consolidated_temp.csv',
                      header=False,
                      index=False,
                      mode='a')

    df = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                     'county_consolidated_temp.csv')
    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.to_csv(_CODEDIR + PROCESS_AS_IS_DIR +
              'county_consolidated_as_is_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AS_IS_DIR \
        + 'county_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AS_IS_DIR + 'county_consolidated_temp.csv')

    county_file = [
        _CODEDIR + PROCESS_AS_IS_DIR + '1990_2000/county/county_1990_2000.csv',
        _CODEDIR + PROCESS_AGG_DIR + '1990_2000/county/county_1990_2000.csv',
        _CODEDIR + PROCESS_AGG_DIR + '2000_2010/county/county_2000_2010.csv',
        _CODEDIR + PROCESS_AGG_DIR + '2010_2020/county/county_2010_2020.csv'
    ]

    for file in county_file:
        df = pd.read_csv(file)
        df = df.melt(id_vars=['YEAR', 'LOCATION'],
                     var_name='SV',
                     value_name='OBSERVATION')
        df.replace({"SV": stat_var_col_mapping}, inplace=True)
        df["SV"] = 'dcid:' + df["SV"]

        if file == county_file[0]:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'county_consolidated_temp.csv',
                      header=True,
                      index=False)
        else:
            df.to_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'county_consolidated_temp.csv',
                      header=False,
                      index=False,
                      mode='a')

    df = pd.read_csv(_CODEDIR + PROCESS_AGG_DIR +
                     'county_consolidated_temp.csv')
    df.sort_values(by=['LOCATION', 'SV', 'YEAR'], inplace=True)
    df.insert(3, 'MEASUREMENT_METHOD', 'dcs:dcAggregate/CensusPEPSurvey', True)
    df.to_csv(_CODEDIR + PROCESS_AGG_DIR + 'county_consolidated_agg_final.csv',
              header=True,
              index=False)

    if os.path.exists(_CODEDIR + PROCESS_AGG_DIR \
        + 'county_consolidated_temp.csv'):
        os.remove(_CODEDIR + PROCESS_AGG_DIR + 'county_consolidated_temp.csv')


def consolidate_all_geo_files():
    '''
    Consolidate National, State and County files into single file
    This function generates final csv file for both as-is and agg
    data processing which will be used for importing into DC.

    Output files are written to /output_files/ folder
    '''

    df1 = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'national_consolidated_as_is_final.csv')

    df2 = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'state_consolidated_as_is_final.csv')

    df3 = pd.read_csv(_CODEDIR + PROCESS_AS_IS_DIR +
                      'county_consolidated_as_is_final.csv')

    df = pd.concat([df1, df2, df3])
    df.to_csv(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh.csv',
              header=True,
              index=False)

    df1 = pd.read_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'national_consolidated_agg_final.csv')

    df2 = pd.read_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'state_consolidated_agg_final.csv')

    df3 = pd.read_csv(_CODEDIR + PROCESS_AGG_DIR +
                      'county_consolidated_agg_final.csv')

    df = pd.concat([df1, df2, df3])
    df.to_csv(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh_agg.csv',
              header=True,
              index=False)


def consolidate_files():
    '''
    Consolidate National, State and County files into single file
    '''
    consolidate_county_files()
    consolidate_state_files()
    consolidate_national_files()
    consolidate_all_geo_files()


def process_files(download_dir):
    '''
    Process county files from 1990 - 2020
    '''
    process_county_files(download_dir)
    process_state_files(download_dir)
    process_national_files()


def create_output_n_process_folders():
    '''
    Create directories for processing data and saving final output
    '''
    for d in working_directories:
        os.system("mkdir -p " + _CODEDIR + d)


def main(download_dir=DOWNLOAD_DIR):
    '''
    Produce As Is and Agg output files for National, State and County
    '''
    create_output_n_process_folders()
    process_files(download_dir)
    consolidate_files()


if __name__ == '__main__':
    main()
