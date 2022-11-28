import os
import numpy as np
import pandas as pd


# get complete DateTimeIndex for simulation period from .json file
def get_datetime_index(uc_dir, nr_months):
    dt_fpaths, dt_index = [], []
    for root, dir, files in os.walk(top=uc_dir, topdown=True):
        dt_fpaths += [os.path.join(root,f) for f in files if 'price_energy_day.json' in f]
        if len(dt_fpaths) == nr_months:
            break
    if len(dt_fpaths) != nr_months:
        raise Exception(f'ERROR: Did not find {nr_months} price_energy_day.json files.')
    for i in dt_fpaths:
        df = pd.read_json(i, orient='index')['price-energy-day']
        dt_index += [pd.to_datetime(df['Germany'][i][0]['time']) for i in range(len(df['Germany']))]
        
    return sorted(dt_index)

def get_files_for_uc(uc_nr, uc_dir, nr_months):
    fpaths = []
    # collect bottom-level entities' (house/wind/industry) filepaths
    for root, dir, files in os.walk(top = uc_dir, topdown=True):
        if uc_nr in ['0','1','5']:
            fpaths += [os.path.join(root,f) for f in files if 'member-' in f if '-trades.csv' in f]
        else:
            fpaths += [os.path.join(root,f) for f in files if any(f'region-{reg}-ec{ec}-trades.csv' in f for reg in range(1,7) for ec in range(6))]
    # for each entity, there must be n files (n = nr_months)
    assert len(fpaths) % nr_months != 0, f'ERROR: {len(fpaths)} filepaths, not divisible by {nr_months} months. Files are missing.'
    
    return fpaths
    
def get_combined_df_p(p_paths, uc_nr):
    dfs_p = []
    # construct df_p from monthly dfs_p
    for p_path in p_paths:
        dfs_p.append(pd.read_csv(p_path, index_col=0).drop(['rate [ct./kWh]','creation_time', 'matching_requirements'], axis=1))
    df_p = pd.concat(dfs_p, axis=0)
    # apply column transformations
    df_p.index = pd.to_datetime([i.split('+')[0].replace('T', ' ') for i in df_p.index])
    df_p.seller, df_p.buyer = df_p.seller.str.lower(), df_p.buyer.str.lower()
    if uc_nr in ['0', '1', '5']:
        df_p.seller = [i.split('mm_')[-1].split('_house')[0] if 'mm_' in i else i for i in df_p.seller]
        df_p.buyer = [i.split('mm_')[-1].split('_house')[0] if 'mm_' in i else i for i in df_p.buyer]
    df_p.sort_index(inplace=True)
    
    return df_p

def trades_dfs_grouper(df_out_index, df_to_group):
    df_to_group = df_to_group.groupby(df_to_group.index)['energy [kWh]'].sum()
    df_grouped = df_to_group.reindex(df_out_index, fill_value=0)
    
    return df_grouped

# get_p_trades_dfs() not used
def get_p_trades_dfs(p, df_e, df_out_index):
    p_to_cs = trades_dfs_grouper(df_out_index, df_e[(df_e.seller.isin(p)) & (~df_e.buyer.isin(p))])
    cs_to_p = trades_dfs_grouper(df_out_index, df_e[(~df_e.seller.isin(p)) & (df_e.buyer.isin(p))])
    
    return p_to_cs, cs_to_p

def get_c_trades_dfs(c, p, cs, df_e, df_out_index):
    cs_without_c = cs - c
    #c_to_p = trades_dfs_grouper(df_out_index, df_e[(df_e.seller.isin(c)) & (df_e.buyer.isin(p))])
    p_to_c = trades_dfs_grouper(df_out_index, df_e[(df_e.seller.isin(p)) & (df_e.buyer.isin(c))])
    #c_to_cs = trades_dfs_grouper(df_out_index, df_e[(df_e.seller.isin(c)) & (df_e.buyer.isin(cs_without_c))])
    cs_to_c = trades_dfs_grouper(df_out_index, df_e[(df_e.seller.isin(cs_without_c)) & (df_e.buyer.isin(c))])
    green_in, grey_in = cs_to_c, p_to_c
    
    return green_in, grey_in
    
def co2_footprint(e_green_in, e_grey_in):
    # taken from: https://fimrc.sharepoint.com/:x:/t/paper-VIdES/EZnJhlevmuVMi3knXpjlRuIBcDw-HWPnGKyYyXgRi7xe6g?e=7Q4Xhm
    gco2e_kWh_green, gco2e_kWh_grey = 27.8180, 181.79
    # convert g to kg (/1000)
    e_co2e = ((e_green_in * gco2e_kWh_green) + (e_grey_in * gco2e_kWh_grey)) / 1000
    
    return e_co2e

def share_renewable(e_green_in, e_grey_in):
    e_share_ren = e_green_in / (e_green_in + e_grey_in)
    e_share_ren = e_share_ren.fillna(0)
    
    return e_share_ren

def rename_e_series(e_green_in, e_grey_in, e_co2e, e_share_green, e):
    e_green_in.name, e_grey_in.name = f'{e}_green_energy [kWh]', f'{e}_grey_energy [kWh]'
    e_co2e.name, e_share_green.name = f'{e}_co2e_emissions [kg]', f'{e}_share_renewable [%]'
    
    return e_green_in, e_grey_in, e_co2e, e_share_green
    
def get_childrens_co2(uc_nr, uc_dir, nr_months):
    # create empty df with date time index
    df_out = pd.DataFrame(index=get_datetime_index(uc_dir, nr_months))
    df_out_index = df_out.index
    # obtain all relevant -trades.csv files for uc_nr
    fpaths = get_files_for_uc(uc_nr, uc_dir, nr_months)
    # get lists of all months filepaths for all parents
    parents_paths = [fpaths[i::int(len(fpaths)/nr_months)] for i in range(int(len(fpaths)/nr_months))]
    cs_names = []
    # combine each parent's list of paths to one df
    for p_paths in parents_paths:
        df_p = get_combined_df_p(p_paths, uc_nr)
        # get children (cs) and parent (p)
        all_entities = set(df_p.seller) | set(df_p.buyer)
        cs = set([i for i in df_p.seller if '_house' in i] + [i for i in df_p.buyer if '_house' in i])
        p = all_entities - cs
        for c in cs:
            cs_names.append(c)
            # get energy received from other children (green) and from parent (grey)
            c_green_in, c_grey_in = get_c_trades_dfs({c}, p, cs, df_p, df_out_index)
            # calculate emissions based on factors for green and grey energy [g/kWh]
            c_co2e = co2_footprint(c_green_in, c_grey_in)
            # calculate share renewables, i.e. share green energy [%]
            c_share_green = share_renewable(c_green_in, c_grey_in)
            # add children to series name
            c_green_in, c_grey_in, c_co2e, c_share_green = rename_e_series(c_green_in, c_grey_in, c_co2e, c_share_green, c)
            # append to df_out
            df_out = pd.concat([df_out, pd.concat([c_green_in, c_grey_in, c_co2e, c_share_green], axis=1)], axis=1)

    return df_out, cs_names
        
def aggregate_entity(df_temp, e_name, level=None):
    if level is None or level not in ['ec', 'region', 'germany']:
        raise Exception('ERROR: Please specify level. Must be in [ec, region, germany].')
    # get all relevant houses' data
    if level == 'ec':
        df_temp_e = df_temp[[i for i in df_temp.columns if f'{e_name}_house' in i if 'energy [kWh]' in i]]
    elif level == 'region':
        df_temp_e = df_temp[[i for i in df_temp.columns if f'{e_name}' in i if not '_house' in i if 'energy [kWh]' in i]]
    else:
        df_temp_e = df_temp[[i for i in df_temp.columns if not '_house' in i if not 'ec' in i if 'energy [kWh]' in i]]        
    # aggregate total green and grey energy
    e_green_in, e_grey_in = df_temp_e[[i for i in df_temp_e if 'green' in i]].sum(axis=1), df_temp_e[[i for i in df_temp_e if 'grey' in i]].sum(axis=1)
    # calculate co2 footprint of e
    e_co2e = co2_footprint(e_green_in, e_grey_in)
    # calculate share renewable of e
    e_share_green = share_renewable(e_green_in, e_grey_in)
    # rename series
    e_green_in, e_grey_in, e_co2e, e_share_green = rename_e_series(e_green_in, e_grey_in, e_co2e, e_share_green, e_name)
    # insert to df_temp
    df_temp = pd.concat([df_temp, pd.concat([e_green_in, e_grey_in, e_co2e, e_share_green], axis=1)], axis=1)
    
    return df_temp


def kpi_calculation(home_path, uc_nr):
    nr_months = 4
    # obtain use case number (uc_nr)
    uc_nr = str(uc_nr)
    assert uc_nr in ['0','1','2','2-1','2-2','3','4','5','6'], f'ERROR: uc_nr is {uc_nr}, must be in [0,1,2,2-1,2-2,3,4,5,6]'
    if uc_nr == '0':
        uc_dir = [i.path for i in os.scandir(home_path) if i.is_dir() if 'base' in i.name.lower() if 'case' in i.name.lower()][0]
    else:
        uc_dir = [i.path for i in os.scandir(home_path) if i.is_dir() if f'case_{uc_nr}' in i.name.lower()][0]
    # get bottom-level df_out_raw for aggregation (#cols: #entities * 4) and list of all childrens' names (cs_names)
    df_temp, cs_names = get_childrens_co2(uc_nr, uc_dir, nr_months)
    for r in range(1,7):
        # aggregate all houses to ecs
        for e in range(6):
            e_name = f'region_{r}_ec{e}'
            df_temp = aggregate_entity(df_temp, e_name, level = 'ec')
        # aggregate all ecs to regions
        r_name = f'region_{r}'
        df_temp = aggregate_entity(df_temp, r_name, level='region')
    # aggregate all regions to germany
    g_name = 'germany'
    df_temp = aggregate_entity(df_temp, g_name, level='germany')
    
    return df_temp
        
    
    