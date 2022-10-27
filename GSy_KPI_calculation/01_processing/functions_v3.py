import os
import numpy as np
import pandas as pd
import functools as func
import warnings


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


def avg_p_houses_helper(month_subset_paths):
    dfs_temp = []
    for i in month_subset_paths:
        df_temp = pd.read_csv(i).iloc[:, np.r_[:2, -1]]
        asset_name = i.split('\\')[-1].split('.csv')[0].replace('-', '_')
        df_temp.columns = df_temp.columns.to_list()[:1] + [f'{asset_name}_' + col for col in df_temp.columns[1:]]
        dfs_temp.append(df_temp)
    # merge dfs
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        avg_price_houses = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)

    return avg_price_houses


def avg_p_houses(use_case_home_path):
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
        months_dfs.append(avg_p_houses_helper(month_subset_paths))
    # concatenate month_dfs, sorted by ascending date

    return pd.concat(months_dfs).sort_values(by='slot')


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
        avg_price_members = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)

    return avg_price_members


def avg_p_members(use_case_home_path):
    # get all relevant file paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if all(x in f for x in ('member', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'id', 'load', 'ev', 'mm'))]
        all_filenames += [f for f in file if all(x in f for x in ('member', '.csv')) if
                          not any(x in f for x in ('trades', 'bids', 'offers', 'id', 'load', 'ev', 'mm'))]
    # for each month, construct subset_df
    months_dfs = []
    nr_months = all_filenames.count(all_filenames[0])
    l = len(all_filenames)
    for i in range(nr_months):
        month_subset_paths = all_filepaths[int(l / nr_months) * i: int(l / nr_months) * (i + 1)]
        months_dfs.append(avg_p_houses_helper(month_subset_paths))
    # concatenate month_dfs, sorted by ascending date

    return pd.concat(months_dfs).sort_values(by='slot')
    return


def get_kpi_dict(homepath, kpi):
    os.chdir(homepath)
    kpi_dict = dict()
    for usecase in [file for file in os.listdir() if 'MACOSX' not in file if 'case' in file.lower()]:
        os.chdir(usecase)
        if kpi == 1:
            usecase_dict = {
                'germany': avg_p_germany(os.getcwd()),
                'regions': avg_p_regions(os.getcwd()),
                'ecs': avg_p_ecs(os.getcwd()),
                'members': avg_p_houses(os.getcwd())
            }
        elif kpi == 2:
            total_cost_list = tot_cost(os.getcwd())
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
        df.name.isin([name for name in names if name in [f'Region_{i}_EC{j}' for i in range(1, 7) for j in range(6)]])]
    dict_out['house'] = df[~df.name.isin(
        [name for name in names if any(x in name for x in ('Germany', 'ID', 'MM', 'Member'))]) & df.name.isin(
        [name for name in names if 'house' in name])]
    dict_out['asset'] = df[
        ~df.name.isin([name for name in names if any(x in name for x in ('Germany', 'MM', 'Member'))]) & df.name.isin(
            [name for name in names if any(x in name for x in ('ID', 'Load'))])]

    return dict_out


# creates separate dfs an entity's trading partners (p_to_c, c_to_p, etc.)
def share_renewable_df_grouper(df_out, df):
    df.index = [i.split(':00+')[0] for i in df.index.tolist()]
    df = df.groupby(df.index)[['energy [kWh]']].sum()
    df = df.reindex(df_out.index, fill_value=0)

    return df


# df_out: final outcome data frame, df: entity_df, p: entity_name
def share_renewable_helper(df_out, df_mm, df, p, co2=False):
    p = p.replace('member', 'mm') if 'member' in p else p
    p = p.replace('germany', 'mm-germany') if 'germany' in p else p
    # get children
    sellers = [i for i in df.seller.unique() if 'mm-' not in i and i != p]
    buyers = [i for i in df.buyer.unique() if 'mm-' not in i and i != p]
    children = pd.Series([*sellers, *buyers]).unique()

    # top level calculation: neglect mm and calculate share for only c
    if 'mm-' in p:
        c = 'germany' if 'grid' in children else children[0]
        p_to_cs = share_renewable_df_grouper(df_out, df[(df.seller.str.contains(p) & (~df.buyer.str.contains(p)))][
            ['energy [kWh]']])
        cs_to_p = share_renewable_df_grouper(df_out, df[(~df.seller.str.contains(p) & (df.buyer.str.contains(p)))][
            ['energy [kWh]']])
        net_p_to_cs = p_to_cs - cs_to_p
        # prepare copy of df_out where calculations are made
        df_out_copy = df_out.copy()
        df_out_copy[f'{c}_net_p_to_c'] = net_p_to_cs
        df_out_copy[f'{c}_p_to_c'] = p_to_cs
        # perform matrix calculations based on selected slice
        df_out_copy.loc[df_out_copy[f'{c}_net_p_to_c'] > 0, [c]] = df_out_copy[f'{c}_net_p_to_c'] / df_out_copy[
            f'{c}_p_to_c'] * df_mm['mm_grey_energy [%]']
        df_out_copy.loc[df_out_copy[f'{c}_net_p_to_c'] <= 0, [c]] = 0
        # rename resulting columns and insert into df_out
        if co2:
            df_out_copy.rename({f'{c}_net_p_to_c': f'{c}_grey_energy [kWh]', f'{c}': f'{c}_grey_energy [%]'}, axis=1,
                               inplace=True)
            df_out = pd.concat([df_out, df_out_copy[[f'{c}_grey_energy [kWh]', f'{c}_grey_energy [%]']]], axis=1)
        else:
            df_out = pd.concat([df_out, df_out_copy[c]], axis=1)
    else:
        for c in children:
            p_to_c = share_renewable_df_grouper(df_out, df[(df.seller == p) & (df.buyer == c)][['energy [kWh]']])
            c_to_p = share_renewable_df_grouper(df_out, df[(df.seller == c) & (df.buyer == p)][['energy [kWh]']])
            net_p_to_c = p_to_c - c_to_p
            cs_to_c = share_renewable_df_grouper(df_out, df[(df.seller != p) & (df.seller != c) & (df.buyer == c)][
                ['energy [kWh]']])
            c_to_cs = share_renewable_df_grouper(df_out,
                                                 df[(df.seller == c) & (df.buyer != p) & (df.buyer != c)][
                                                     ['energy [kWh]']])
            net_cs_to_c = cs_to_c - c_to_cs
            # prepare copy of df_out where calculations are made
            c_name = c if 'id' not in c else p + '_' + c
            df_out_copy = df_out.copy()
            df_out_copy[f'{c_name}_net_p_to_c'] = net_p_to_c
            df_out_copy[f'{c_name}_net_cs_to_c'] = net_cs_to_c
            # perform matrix calculations based on selected slice
            if p in df_out.columns.tolist():
                df_out_copy.loc[
                    (df_out_copy[f'{c_name}_net_p_to_c'] > 0) & (df_out_copy[f'{c_name}_net_cs_to_c'] <= 0), [c_name]] = \
                    df_out[p]
                p_share_grey_electricity = df_out[p]
                df_out_copy.loc[
                    (df_out_copy[f'{c_name}_net_p_to_c'] > 0) & (df_out_copy[f'{c_name}_net_cs_to_c'] > 0), [
                        c_name]] = p_share_grey_electricity * (df_out_copy[f'{c_name}_net_p_to_c'] / (
                        df_out_copy[f'{c_name}_net_p_to_c'] + df_out_copy[f'{c_name}_net_cs_to_c']))
                df_out_copy.loc[(df_out_copy[f'{c_name}_net_p_to_c'] <= 0), [c_name]] = 0
            else:
                df_out_copy.loc[
                    (df_out_copy[f'{c_name}_net_p_to_c'] > 0) & (df_out_copy[f'{c_name}_net_cs_to_c'] <= 0), [
                        c_name]] = 1
                p_share_grey_electricity = 1
                df_out_copy.loc[
                    (df_out_copy[f'{c_name}_net_p_to_c'] > 0) & (df_out_copy[f'{c_name}_net_cs_to_c'] > 0), [
                        c_name]] = p_share_grey_electricity * (df_out_copy[f'{c_name}_net_p_to_c'] / (
                        df_out_copy[f'{c_name}_net_p_to_c'] + df_out_copy[f'{c_name}_net_cs_to_c']))
                df_out_copy.loc[(df_out_copy[f'{c_name}_net_p_to_c'] <= 0), [c_name]] = 0
            if co2:
                # calculate grey energy [kWh] based on parent share, rename columns and insert into df_out
                df_out_copy[f'{c_name}_grey_energy [kWh]'] = df_out_copy[f'{c_name}_net_p_to_c'] * df_out_copy[
                    f'{c_name}']
                df_out_copy.rename({f'{c_name}': f'{c_name}_grey_energy [%]'}, axis=1, inplace=True)
                df_out = pd.concat([df_out, df_out_copy[[f'{c_name}_grey_energy [kWh]', f'{c_name}_grey_energy [%]']]],
                                   axis=1)
            else:
                # insert result into df_out as new column
                df_out = pd.concat([df_out, df_out_copy[c_name]], axis=1)

    return df_out


def get_use_case_nr(use_case_home_path):
    s_use_case = use_case_home_path.split('\\')[-1].lower()
    if 'base' in s_use_case or '0' in s_use_case:
        i = 0
    elif '1' in s_use_case:
        i = 1
    elif '2' in s_use_case:
        i = 2
    elif '3' in s_use_case:
        i = 3
    elif '4' in s_use_case:
        i = 4
    elif '5' in s_use_case:
        i = 5
    else:
        i = None

    return i


def share_renewables_nonempty_files(all_filepaths, use_case_home_path):
    nonempty_files_paths, empty_files_paths = [], []
    # detect use case from use_case_home_path
    use_case_nr = get_use_case_nr(use_case_home_path)
    if use_case_nr is None:
        print('no use case found in use_case_home_path')
        return nonempty_files_paths, empty_files_paths
    # base case, use case 1, 4, and 5
    if use_case_nr < 2 or use_case_nr >= 4:
        nonempty_files_paths = [i for i in all_filepaths if any(x in i.split('\\')[-1] for x in ('member', 'house'))]
        empty_files_paths = list(set(all_filepaths) - set(nonempty_files_paths))
    elif use_case_nr == 2:
        nonempty_files_paths = all_filepaths
    elif use_case_nr == 3:
        nonempty_files_paths = [i for i in all_filepaths if any(x in i.split('\\')[-1] for x in ('member', 'region'))]
        empty_files_paths = list(set(all_filepaths) - set(nonempty_files_paths))
    else:
        print('no use case found in use_case_home_path')

    return nonempty_files_paths, empty_files_paths


def prepare_filepaths(use_case_home_path):
    # obtain all months' time slots from .json files and combine them to index
    json_filepaths, timeslots = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        json_filepaths += [os.path.join(root, f) for f in file if 'price_energy_day.json' in f]
    for j in json_filepaths:
        df = pd.read_json(j)
        timeslots += [df['Germany']['price-energy-day'][i][0]['time'] for i in
                      range(len(df['Germany']['price-energy-day']))]
    timeslots.sort()
    df_out = pd.DataFrame(index=timeslots)

    # get (non-)empty files' paths
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_home_path, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if 'trades.csv' in f]
        all_filenames += [f for f in file if 'trades.csv' in f]
    nonempty_files_paths, empty_files_paths = share_renewables_nonempty_files(all_filepaths, use_case_home_path)
    nr_months, l = all_filenames.count(all_filenames[0]), len(nonempty_files_paths)

    return nonempty_files_paths, empty_files_paths, nr_months, l, df_out


def share_renewables_aggregate_empty_entities_helper(df_out, reg, ec):
    # obtain sub_df with sub-entities relevant for aggregation
    if reg is not None and ec is not None:
        name = f'region-{reg}-ec{ec}'
        sub_df = df_out[[i for i in df_out.columns.tolist() if name in i if 'id' not in i]]
    if reg is not None and ec is None:
        name = f'region-{reg}'
        sub_df = df_out[
            [i for i in df_out.columns.tolist() if name in i if any(f'ec{x}_' in i for x in (range(6)))]]
    if reg is None and ec is None:
        name = 'germany'
        sub_df = df_out[[i for i in df_out.columns.tolist() if any(f'region-{x}_' in i for x in range(1, 7))]]
    # calculate sub-entities' energy consumed and volume-weighted average share renewables
    df_name = pd.DataFrame(index=sub_df.index)
    df_name[f'{name}_grey_energy [kWh]'] = sub_df[sub_df.columns[::2]].sum(axis=1)
    df_name[f'{name}_grey_energy [%]'] = 0
    volume_kWh = abs(sub_df[sub_df.columns[::2]]).sum(axis=1)
    # add every sub-entity pair's volume-weighted share grey_energy [%]
    for i in range(0, len(sub_df.columns) - 1, 2):
        t = sub_df[sub_df.columns[i:i + 2]]
        df_name[f'{name}_grey_energy [%]'] += abs(t[t.columns[0]]) / volume_kWh * t[t.columns[-1]]
    # combine df_name with df_out
    df_out = pd.concat([df_out, df_name], axis=1)

    return df_out


def share_renewables_aggregate_empty_entities(df_out, use_case_nr):
    if use_case_nr not in list(range(6)):
        print(f'invalid use_case_nr: {use_case_nr}')
        return df_out
    if use_case_nr == 2:
        return df_out
    if use_case_nr < 2 or use_case_nr >= 4:
        # ecs
        for reg in range(1, 7):
            for ec in range(6):
                df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg, ec)
        # regions
        for reg in range(1, 7):
            df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg, ec=None)
        # germany
        df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg=None, ec=None)
    if use_case_nr == 3:
        # germany
        df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg=None, ec=None)

    return df_out


def share_renewables(use_case_home_path):
    # calculate net renewable share for mm over all entities that trade with it
    df_mm = share_renewable_get_mm_quantity(use_case_home_path)
    # for use case, extract all (non-)relevant filepaths and construct empty df_out with all time steps
    nonempty_files_paths, empty_files_paths, nr_months, l, df_out = prepare_filepaths(use_case_home_path)
    # combine each entity's monthly data frames and hand them over to shares_renewable_helper for calculation
    for i in range(int(l / nr_months)):
        entity_monthly_dfs = []
        entity_monthly_filepaths = nonempty_files_paths[i::int(l / nr_months)]
        for j in entity_monthly_filepaths:
            df_temp = pd.read_csv(j).drop(['creation_time', 'matching_requirements', 'rate [ct./kWh]'], axis=1)
            df_temp.seller = [i.lower().replace('_', '-') for i in df_temp.seller]
            df_temp.buyer = [i.lower().replace('_', '-') for i in df_temp.buyer]
            entity_monthly_dfs.append(df_temp)
        entity_name = entity_monthly_filepaths[0].split('\\')[-1].split('-trades.csv')[0]
        entity_df = pd.concat(entity_monthly_dfs).sort_values(by='slot').reset_index(drop=True)
        entity_df.set_index(['slot'], inplace=True)
        # hand over entity_df to helper function which inserts entity's result as new column into df_out
        df_out = share_renewable_helper(df_out, df_mm, entity_df, entity_name)

    return df_out


def share_renewable_get_mm_quantity(use_case_home_path):
    # get all relevant files: extract all (non-)relevant filepaths and construct empty df_out with all time steps
    nonempty_files_paths, empty_files_paths, nr_months, l, df_out = prepare_filepaths(use_case_home_path)
    use_case_nr = get_use_case_nr(use_case_home_path)
    if use_case_nr != 2:
        mm_filepaths = [i for i in nonempty_files_paths if 'member' in i.split('\\')[-1]]
    else:
        mm_filepaths = [i for i in nonempty_files_paths if 'germany-trades.csv' in i]
    l = len(mm_filepaths)
    # set up df_mm
    df_mm = df_out.copy()
    df_mm[['mm_to_cs', 'cs_to_mm']] = 0, 0
    # for each entity involved with mm, combine all months' data frames
    entity_dfs = []
    for i in range(int(l / nr_months)):
        entity_monthly_dfs = []
        entity_monthly_filepaths = mm_filepaths[i::int(l / nr_months)]
        for j in entity_monthly_filepaths:
            df_temp = pd.read_csv(j).drop(['creation_time', 'matching_requirements', 'rate [ct./kWh]'], axis=1)
            df_temp.seller = [i.lower().replace('_', '-') for i in df_temp.seller]
            df_temp.buyer = [i.lower().replace('_', '-') for i in df_temp.buyer]
            entity_monthly_dfs.append(df_temp)
        entity_name = entity_monthly_filepaths[0].split('\\')[-1].split('-trades.csv')[0]
        entity_df = pd.concat(entity_monthly_dfs).sort_values(by='slot').reset_index(drop=True)
        entity_dfs.append(entity_df)
    # combine all dfs and calculate mm trading volumes
    mm_df = pd.concat(entity_dfs, axis=0).sort_values(by='slot')
    mm_df.set_index(['slot'], inplace=True)
    df_mm['mm_to_cs'] = share_renewable_df_grouper(df_out, mm_df[
        (mm_df.seller.str.contains('mm') & (~mm_df.buyer.str.contains('mm')))][['energy [kWh]']])
    df_mm['cs_to_mm'] = share_renewable_df_grouper(df_out, mm_df[
        (~mm_df.seller.str.contains('mm') & (mm_df.buyer.str.contains('mm')))][['energy [kWh]']])
    df_mm['mm_grey_energy [kWh]'] = df_mm['mm_to_cs'] - df_mm['cs_to_mm']
    df_mm.loc[df_mm['mm_grey_energy [kWh]'] > 0, ['mm_grey_energy [%]']] = df_mm['mm_grey_energy [kWh]'] / df_mm[
        'mm_to_cs']
    df_mm.loc[df_mm['mm_grey_energy [kWh]'] <= 0, ['mm_grey_energy [%]']] = 0

    return df_mm


def co2_emissions(use_case_home_path):
    # calculate net renewable share for mm over all entities that trade with it
    df_mm = share_renewable_get_mm_quantity(use_case_home_path)
    # for use case, extract all (non-)relevant filepaths and construct empty df_out with all time steps
    nonempty_files_paths, empty_files_paths, nr_months, l, df = prepare_filepaths(use_case_home_path)
    # combine each entity's monthly data frames and hand them over to shares_renewable_helper for calculation
    for i in range(int(l / nr_months)):
        entity_monthly_dfs = []
        entity_monthly_filepaths = nonempty_files_paths[i::int(l / nr_months)]
        for j in entity_monthly_filepaths:
            df_temp = pd.read_csv(j).drop(['creation_time', 'matching_requirements', 'rate [ct./kWh]'], axis=1)
            df_temp.seller = [i.lower().replace('_', '-') for i in df_temp.seller]
            df_temp.buyer = [i.lower().replace('_', '-') for i in df_temp.buyer]
            entity_monthly_dfs.append(df_temp)
        entity_name = entity_monthly_filepaths[0].split('\\')[-1].split('-trades.csv')[0]
        entity_df = pd.concat(entity_monthly_dfs).sort_values(by='slot').reset_index(drop=True)
        entity_df.set_index(['slot'], inplace=True)
        # hand over entity_df to helper function which inserts entity's result as new column into df_out
        df = share_renewable_helper(df, df_mm, entity_df, entity_name, co2=True)
        # aggregate sub-entities to fill missing top-level entities' values (depends on use case)
        use_case_nr = get_use_case_nr(use_case_home_path)
        df_out = share_renewables_aggregate_empty_entities(df, use_case_nr)

    return df_out
