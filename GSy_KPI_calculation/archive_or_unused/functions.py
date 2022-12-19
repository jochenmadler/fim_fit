import os
import numpy as np
import pandas as pd
import functools as func
import warnings


def avg_price_ger(homepath_usecase):
    os.chdir(homepath_usecase)
    avg_price_ger = pd.read_csv('germany.csv').iloc[:, np.r_[:2, -1]]
    avg_price_ger.columns = avg_price_ger.columns.to_list()[:1] + [f'germany_' + col for col in
                                                                   avg_price_ger.columns[1:]]
    os.chdir(homepath_usecase)

    return avg_price_ger


def avg_p_germany_helper(file_path):
    avg_price_ger = pd.read_csv(file_path).iloc[:, np.r_[:2, -1]]
    avg_price_ger.columns = avg_price_ger.columns.to_list()[:1] + [f'germany_' + col for col in
                                                                   avg_price_ger.columns[1:]]

    return avg_price_ger


def avg_p_germany(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if 'germany.csv' in f if 'mm' not in f]
        all_filenames += [f for f in file if 'germany.csv' in f if 'mm' not in f]
    # concatenate month_dfs, sorted by ascending date
    months_dfs = []
    for file_path in all_filepaths:
        months_dfs.append(avg_p_germany_helper(file_path))
    return pd.concat(months_dfs).sort_values(by='slot')


def avg_price_regions(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('germany/')
    dfs_temp = []
    # use case 2 caveat: enter grid folder first
    if 'grid' in os.listdir():
        os.chdir('grid/')
    # extract .csv files
    for file in [file for file in os.listdir() if 'MACOSX' not in file]:
        if '.csv' in file and not any(x in file for x in ('bids', 'offers', 'trades', 'mm')):
            df_temp = pd.read_csv(file).iloc[:, np.r_[:2, -1]]
            region = int(file.split(".")[0].split("-")[-1])
            df_temp.columns = df_temp.columns.to_list()[:1] + [f'region{region}_' + col for col in df_temp.columns[1:]]
            dfs_temp.append(df_temp)
    # merge dfs
    avg_price_regions = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)
    os.chdir(homepath_usecase)

    return avg_price_regions


def avg_p_regions_helper(month_subset_paths):
    dfs_temp = []
    for i in month_subset_paths:
        df_temp = pd.read_csv(i).iloc[:, np.r_[:2, -1]]
        region = i.split('\\')[-1].split('.')[0].split('-')[-1]
        df_temp.columns = df_temp.columns.to_list()[:1] + [f'region{region}_' + col for col in df_temp.columns[1:]]
        dfs_temp.append(df_temp)
    # merge dfs
    avg_price_regions = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)

    return avg_price_regions


def avg_p_regions(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if all(x in f for x in ('region', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'ec', 'member', 'id', 'industry'))]
        all_filenames += [f for f in file if all(x in f for x in ('region', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'ec', 'member', 'id', 'industry'))]
    # for each month, construct subset_df
    months_dfs = []
    nr_months = all_filenames.count(all_filenames[0])
    l = len(all_filenames)
    for i in range(nr_months):
        month_subset_paths = all_filepaths[int(l / nr_months) * i: int(l / nr_months) * (i + 1)]
        months_dfs.append(avg_p_regions_helper(month_subset_paths))
    # concatenate month_dfs, sorted by ascending date

    return pd.concat(months_dfs).sort_values(by='slot')


def avg_p_ecs_helper(month_subset_paths):
    dfs_temp = []
    for i in month_subset_paths:
        df_temp = pd.read_csv(i).iloc[:, np.r_[:2, -1]]
        region, ec = i.split('\\')[-1].split('.')[0].split('-ec')
        region = region.replace('-', '_')
        df_temp.columns = df_temp.columns.to_list()[:1] + [f'{region}_ec{ec}_' + col for col in df_temp.columns[1:]]
        dfs_temp.append(df_temp)
    # merge dfs
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        avg_price_ecs = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)

        return avg_price_ecs


def avg_p_ecs(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if all(x in f for x in ('ec', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'house', 'member', 'id', 'industry'))]
        all_filenames += [f for f in file if all(x in f for x in ('ec', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'house', 'member', 'id', 'industry'))]
    # for each month, construct subset_df
    months_dfs = []
    nr_months = all_filenames.count(all_filenames[0])
    l = len(all_filenames)
    for i in range(nr_months):
        month_subset_paths = all_filepaths[int(l / nr_months) * i: int(l / nr_months) * (i + 1)]
        months_dfs.append(avg_p_ecs_helper(month_subset_paths))
    # concatenate month_dfs, sorted by ascending date

    return pd.concat(months_dfs).sort_values(by='slot')


def avg_price_ecs(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('germany/')
    dfs_temp = []
    # use case 2 caveat: enter grid folder first
    if 'grid' in os.listdir():
        os.chdir('grid/')
    # iterate over region folders
    for region in [i for i in os.listdir() if '.csv' not in i if 'MACOSX' not in i]:
        os.chdir(region)
        # iterate over ecs
        for ecs in [i for i in os.listdir() if 'MACOSX' not in i]:
            if '.csv' in ecs and not any(x in ecs for x in ('bids', 'offers', 'trades')):
                # caveat use case 4: 'region-1-ec0.csv' is named 'member-region-1-ec0.csv' -> skip 'member'
                if 'member' in ecs:
                    region, ec = ecs.split('.')[0].split('-')[2], ecs.split('.')[0].split('-')[-1]
                else:
                    region, ec = ecs.split('.')[0].split('-')[1], ecs.split('.')[0].split('-')[-1]
                df_temp = pd.read_csv(ecs).iloc[:, np.r_[:2, -1]]
                df_temp.columns = df_temp.columns.to_list()[:1] + [f'region{region}_{ec}_' + col for col in
                                                                   df_temp.columns[1:]]
                dfs_temp.append(df_temp)
        os.chdir('..')
    # merge dfs
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        avg_price_ecs = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)
    os.chdir(homepath_usecase)

    return avg_price_ecs


def avg_p_members_helper(month_subset_paths):
    dfs_temp = []
    for i in month_subset_paths:
        df_temp = pd.read_csv(i).iloc[:, np.r_[:2, -1]]
        asset_name = i.split('\\')[-1].split('.csv')[0].replace('-', '_')
        df_temp.columns = df_temp.columns.to_list()[:1] + [f'{asset_name}_' + col for col in df_temp.columns[1:]]
        dfs_temp.append(df_temp)
    # merge dfs
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        avg_price_assets = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)

    return avg_price_assets


def avg_p_members(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if all(x in f for x in ('house', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'id', 'load', 'ev', 'mm', 'member'))]
        all_filenames += [f for f in file if all(x in f for x in ('house', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'id', 'load', 'ev', 'mm', 'member'))]
    # for each month, construct subset_df
    months_dfs = []
    nr_months = all_filenames.count(all_filenames[0])
    l = len(all_filenames)
    for i in range(nr_months):
        month_subset_paths = all_filepaths[int(l / nr_months) * i: int(l / nr_months) * (i + 1)]
        months_dfs.append(avg_p_members_helper(month_subset_paths))
    # concatenate month_dfs, sorted by ascending date

    return pd.concat(months_dfs).sort_values(by='slot')


def avg_price_assets(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('germany/')
    dfs_temp = []
    # use case 2 caveat: enter grid folder first
    if 'grid' in os.listdir():
        os.chdir('grid/')
    # iterate over assets
    for region in [i for i in os.listdir() if '.csv' not in i]:
        os.chdir(region)
        for ec in [i for i in os.listdir() if '.csv' not in i]:
            os.chdir(ec)
            # caveat use case 4: go one level deeper
            if any('mm' in x for x in os.listdir()):
                for ec_2 in [i for i in os.listdir() if '.csv' not in i]:
                    os.chdir(ec_2)
                    # iterate over all assets
                    for file in [i for i in os.listdir() if
                                 '.csv' in i and not any(x in i for x in ('bids', 'offers', 'trades'))]:
                        name = file.split('.csv')[0]
                        df_temp = pd.read_csv(file).iloc[:, np.r_[:2, -1]]
                        df_temp.columns = df_temp.columns.to_list()[:1] + [f'{name}_' + col for col in
                                                                           df_temp.columns[1:]]
                        dfs_temp.append(df_temp)
                    os.chdir('..')
            else:
                for file in [i for i in os.listdir() if
                             '.csv' in i and not any(x in i for x in ('bids', 'offers', 'trades'))]:
                    name = file.split('.csv')[0]
                    df_temp = pd.read_csv(file).iloc[:, np.r_[:2, -1]]
                    df_temp.columns = df_temp.columns.to_list()[:1] + [f'{name}_' + col for col in df_temp.columns[1:]]
                    dfs_temp.append(df_temp)
            os.chdir('..')
        os.chdir('..')
    # merge dfs
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        avg_price_assets = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)
    os.chdir(homepath_usecase)

    return avg_price_assets


def get_kpi_dict(homepath, kpi):
    os.chdir(homepath)
    kpi_dict = dict()
    for usecase in [file for file in os.listdir() if 'MACOSX' not in file if 'case' in file.lower()]:
        os.chdir(usecase)
        if kpi == 1:
            usecase_dict = {
                'germany': avg_price_ger(os.getcwd()),
                'regions': avg_price_regions(os.getcwd()),
                'ecs': avg_price_ecs(os.getcwd()),
                'assets': avg_price_assets(os.getcwd())
            }
        elif kpi == 2:
            total_cost_list = total_cost(os.getcwd())
            usecase_dict = {
                'germany': total_cost_list[1],
                'market_maker': total_cost_list[2],
                'regions': total_cost_list[3],
                'ecs': total_cost_list[4],
                'other_assets': total_cost_list[5]
            }
        kpi_dict[f'{usecase}'] = usecase_dict
        os.chdir(homepath)
    return kpi_dict


def tot_cost_helper(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if 'cumulative_bills.json' in f]
    # read in all months' files, group by agent_name and sum up total cost
    months_dfs = []
    for i in all_filepaths:
        df_month = pd.read_json(i, orient='index')[['name', 'total']]
        months_dfs.append(df_month)

    return pd.concat(months_dfs).groupby('name', as_index=False).sum()


def tot_cost(use_case_home_path):
    # obtain df with all agents' total costs
    df = tot_cost_helper(use_case_home_path)
    # filter out agent category subsets and add to dict_out
    dict_out = dict()
    names = df.name
    dict_out['germany'] = df[df.name.isin([name for name in names if name in 'Germany'])]
    dict_out['mm'] = df[df.name.isin([name for name in names if 'MM' in name])]
    dict_out['region'] = df[df.name.isin([name for name in names if name in [f'Region_{i}' for i in range(1, 7)]])]
    dict_out['ec'] = df[
        df.name.isin([name for name in names if name in [f'Region_{i}_EC{j}' for i in range(1, 7) for j in range(5)]])]
    dict_out['member'] = df[~df.name.isin(
        [name for name in names if any(x in name for x in ('Germany', 'ID', 'MM', 'Member'))]) & df.name.isin(
        [name for name in names if 'house' in name])]
    dict_out['asset'] = df[
        ~df.name.isin([name for name in names if any(x in name for x in ('Germany', 'MM', 'Member'))]) & df.name.isin(
            [name for name in names if any(x in name for x in ('ID', 'Load'))])]

    return dict_out


def total_cost(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('aggregated_results/')
    # read in json file and filter out all names
    df = pd.read_json('cumulative_bills.json').T[['name', 'total']]
    names = df['name']
    # filter out data subsets for germany, mms, regions, ecs, and the rest (members)
    df_germany = df[df.name.isin([name for name in names if name in 'Germany'])]
    df_mms = df[df.name.isin([name for name in names if 'MM' in name])]
    df_regions = df[df.name.isin([name for name in names if name in [f'Region_{i}' for i in range(1, 7)]])]
    df_ecs = df[
        df.name.isin([name for name in names if name in [f'Region_{i}_EC{j}' for i in range(1, 7) for j in range(5)]])]
    df_members = df[(~df.name.isin([name for name in names if name in 'Germany'])) &
                    (~df.name.isin([name for name in names if 'MM' in name])) &
                    (~df.name.isin([name for name in names if name in [f'Region_{i}' for i in range(1, 7)]])) &
                    (~df.name.isin([name for name in names if
                                    name in [f'Region_{i}_EC{j}' for i in range(1, 7) for j in range(5)]]))]
    os.chdir(homepath_usecase)

    return [df, df_germany, df_mms, df_regions, df_ecs, df_members]


def share_renewable_helper(file_path, dict_out):
    df = pd.read_csv(file_path).drop(['creation_time', 'matching_requirements', 'rate [ct./kWh]'], axis=1)
    df.seller = [i.lower().replace('_', '-') for i in df.seller]
    df.buyer = [i.lower().replace('_', '-') for i in df.buyer]
    # p: parent entity name
    p = file_path.split('\\')[-1].split('-trades.csv')[0]
    # special case use case 4:
    if 'member' in p:
        p = p.replace('member', 'mm')
    # get children
    sellers = [i for i in df.seller.unique() if 'mm-' not in i and i != p]
    buyers = [i for i in df.buyer.unique() if 'mm-' not in i and i != p]
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        children = pd.Series(sellers + buyers).unique()
    df.set_index(['slot'], inplace=True)
    for slot, df_slot in df.groupby(level=0):
        if len(dict_out.keys()) < 1 or slot not in dict_out.keys():
            dict_out[slot] = dict()
        # p: parent entity name -> special case for highest level: Germany
        if 'germany' in p:
            p_to_cs = df_slot[df_slot.seller.str.contains(p) &
                              (~df_slot.buyer.str.contains(p))]['energy [kWh]'].sum()
            cs_to_p = df_slot[(~df_slot.seller.str.contains(p)) &
                              (df_slot.buyer.str.contains(p))]['energy [kWh]'].sum()
            net_p_to_cs = p_to_cs - cs_to_p
            p_share_grey_electricity = net_p_to_cs / p_to_cs if net_p_to_cs > 0 else 0
            dict_out[slot][p] = p_share_grey_electricity
        # c: children entity name
        for c in children:
            # net energy child bought from parent
            p_to_c = df_slot[df_slot.seller.str.contains(p) &
                             (df_slot.buyer.str.contains(c))]['energy [kWh]'].sum()
            c_to_p = df_slot[df_slot.seller.str.contains(c) &
                             (df_slot.buyer.str.contains(p))]['energy [kWh]'].sum()
            net_p_to_c = p_to_c - c_to_p
            # net energy child bought from other children
            cs_to_c = df_slot[(~df_slot.seller.str.contains(p)) &
                              (~df_slot.seller.str.contains(c)) &
                              (df_slot.buyer.str.contains(c))]['energy [kWh]'].sum()
            c_to_cs = df_slot[(df_slot.seller.str.contains(c)) &
                              (~df_slot.buyer.str.contains(p)) &
                              (~df_slot.buyer.str.contains(c))]['energy [kWh]'].sum()
            net_cs_to_c = cs_to_c - c_to_cs
            # children's share grey electricity
            if net_p_to_c > 0 and net_cs_to_c <= 0:
                c_share_grey_electricity = 1 if p not in dict_out[slot].keys() else dict_out[slot][p]
            elif net_p_to_c > 0 and net_cs_to_c > 0:
                p_share_grey_electricity = 1 if p not in dict_out[slot].keys() else dict_out[slot][p]
                c_share_grey_electricity = p_share_grey_electricity * (net_p_to_c / (net_p_to_c + net_cs_to_c))
            else:
                c_share_grey_electricity = 0
            # prevent overwriting of assets that are used in multiple houses (e.g. ev-non-commuter)
            c = c if 'id' not in c else p + '_' + c
            dict_out[slot][c] = c_share_grey_electricity

    return dict_out


def share_renewables(homepath_usecase):
    dict_out = dict()
    files = []
    for root, dir, file in os.walk(top=os.getcwd(), topdown=True):
        files += [os.path.join(root, f) for f in file if 'trades.csv' in f]
    for file in files:
        dict_out = share_renewable_helper(file, dict_out)
    df_out = pd.DataFrame.from_dict(dict_out, orient='index')

    # TODO: Multiply share grey energy with electricity mix to obtain share renewable

    return df_out