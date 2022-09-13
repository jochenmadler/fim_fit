import os
import pandas as pd
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
import functools as func


def write_hello():
    print('Hello world.')
    return


def merge_all_region_csv():
    os.chdir('germany')
    dfs = []
    # extract .csv files
    for file in os.listdir():
        if '.csv' in file and not any(x in file for x in ('bids', 'offers', 'trades', 'mm')):
            df_temp = pd.read_csv(file)
            nr = int(file.split(".")[0].split("-")[-1])
            df_temp.columns = df_temp.columns.to_list()[:1] + [f'region_{nr}_' + col for col in df_temp.columns[1:]]
            dfs.append(df_temp)
    # merge dfs
    dfs_merged = func.reduce(lambda left, right: pd.merge(left, right, on=dfs[0].columns[0]), dfs)

    os.chdir('..')
    return dfs_merged


def filter_kpi_df(df, by):
    if by not in ('country', 'regions', 'ecs'):
        print(f'{by} is invalid' r'Please choose between "country", "regions", or "ecs".')
        return None
    if by == 'country':
        return df[df.name.str.contains('Germany')]
    elif by == 'regions':
        return df[df.name.str.contains('E|I|Germany|Wind') is False]
    else:
        return df[df.name.str.contains('EC')]


def calculate_share_EE(df, external_EE_mix=.42 , country=False, region=False, ec=False):
    self_suff = 0
    if country is False:
        print(f'Please specify "country"')
        return
    if country:
        # calculate germany
        if region is False:
            self_suff = df[df.name.str.contains('Germany')]['self_sufficiency'][0]
        else:
            if region not in range(1,7):
                print(f'region must be in [1;6]')
                return None
            # calculate for one region
            if ec is False:
                self_suff = df[(df.name.str.contains('E|I|Germany|Wind') == False) &
                                           (df.name.str.contains(f'{region}'))]['self_sufficiency'][0]
            else:
                if ec not in range(0,5):
                    print('ec must be in [0;4]')
                    return None
                # calculate for one region and one ec
                self_suff = df[df.name.str.contains(f'Region_{region}_EC{ec}')]['self_sufficiency'][0]

    return (1-self_suff) * external_EE_mix + self_suff * 100.0

