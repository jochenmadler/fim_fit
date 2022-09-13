import os
import numpy as np
import pandas as pd
import functools as func
import warnings


def avg_price_ger(homepath_usecase):
    os.chdir(homepath_usecase)
    avg_price_ger = pd.read_csv('germany.csv').iloc[:,np.r_[:2,-1]]
    avg_price_ger.columns = avg_price_ger.columns.to_list()[:1] + [f'germany_' + col for col in
                                                       avg_price_ger.columns[1:]]
    os.chdir(homepath_usecase)

    return avg_price_ger


def avg_price_regions(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('germany/')
    dfs_temp = []
    # use case 2 caveat: enter grid folder first
    if 'grid' in os.listdir():
        os.chdir('grid/')
    # extract .csv files
    for file in os.listdir():
        if '.csv' in file and not any(x in file for x in ('bids', 'offers', 'trades', 'mm')):
            df_temp = pd.read_csv(file).iloc[:,np.r_[:2,-1]]
            region = int(file.split(".")[0].split("-")[-1])
            df_temp.columns = df_temp.columns.to_list()[:1] + [f'region{region}_' + col for col in df_temp.columns[1:]]
            dfs_temp.append(df_temp)
    # merge dfs
    avg_price_regions = func.reduce(lambda left, right: pd.merge(left, right, on=dfs_temp[0].columns[0]), dfs_temp)
    os.chdir(homepath_usecase)

    return avg_price_regions


def avg_price_ecs(homepath_usecase):
    os.chdir(homepath_usecase)
    os.chdir('germany/')
    dfs_temp = []
    # use case 2 caveat: enter grid folder first
    if 'grid' in os.listdir():
        os.chdir('grid/')
    # iterate over region folders
    for region in [i for i in os.listdir() if '.csv' not in i]:
            os.chdir(region)
            # iterate over ecs
            for ecs in os.listdir():
                if '.csv' in ecs and not any(x in ecs for x in ('bids', 'offers', 'trades')):
                    # caveat use case 4: 'region-1-ec0.csv' is named 'member-region-1-ec0.csv' -> skip 'member'
                    if 'member' in ecs:
                        region, ec = ecs.split('.')[0].split('-')[2], ecs.split('.')[0].split('-')[-1]
                    else:
                        region, ec = ecs.split('.')[0].split('-')[1], ecs.split('.')[0].split('-')[-1]
                    df_temp = pd.read_csv(ecs).iloc[:,np.r_[:2,-1]]
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
    for usecase in os.listdir():
        nr = [i for i in usecase if any(str(x) in i for x in range(5))]
        if len(nr) < 1:
            continue
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
        kpi_dict[f'usecase_{nr[0]}'] = usecase_dict
        os.chdir(homepath)
    return kpi_dict


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


def share_renewables(homepath_usecase):
    os.chdir(homepath_usecase)
    # obtain trades with mm -> might need adaption if mm on multiple levels -> df_list
    for file in os.listdir():
        if 'trades' in file:
            all_trades_df = pd.read_csv(file).iloc[:, np.r_[0, 3:6]]
    # filter out trades where mm is selling to an entity other than itself
    trades = all_trades_df[(all_trades_df.seller.isin([seller for seller in all_trades_df.seller if 'MM' in seller])) &
                           (~all_trades_df.buyer.isin([buyer for buyer in all_trades_df.buyer if 'MM' in buyer]))]
    # obtain list of buyers for which to calculate share of renewable energy -> might need to be appended to greater list
    buyers = trades.buyer.unique()
    # obtain kpis for all members below mm level
    os.chdir('aggregated_results/')
    df_kpi = pd.read_json('kpi.json').T.iloc[:, :-6]
    # filter out entities for which to calculate share of renewables
    df_kpi_buyers = df_kpi[df_kpi.name.isin(buyers)]
    total_imported_from_mm_list, total_renewable_consumed_list, share_renewable_consumed_list = [], [], []
    # for each buyer, sum up all energy that was imported by mm
    pd.set_option('display.float_format', str)
    for i in range(len(df_kpi_buyers)):
        buyer = df_kpi_buyers.name[i]
        # calculate kpis
        total_consumed_kw = df_kpi_buyers[df_kpi_buyers.name == buyer].total_energy_demanded_wh[0]
        total_self_consumed_kw = df_kpi_buyers[df_kpi_buyers.name == buyer].total_self_consumption_wh[0]
        total_imported_kw = total_consumed_kw - total_self_consumed_kw
        total_imported_from_mm_kw = trades[trades.buyer == buyer]['energy [kWh]'].sum() * 1000
        total_renewable_consumed_kw = total_self_consumed_kw + total_imported_kw - total_imported_from_mm_kw
        share_renewable_consumed = total_renewable_consumed_kw / total_consumed_kw
        # append to lists
        total_imported_from_mm_list.append(total_imported_from_mm_kw)
        total_renewable_consumed_list.append(total_renewable_consumed_kw)
        share_renewable_consumed_list.append(share_renewable_consumed)
    # append to df_kpi_buyers (suppress warnings)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df_kpi_buyers['total_energy_imported_from_mm_wh'] = total_imported_from_mm_list
        df_kpi_buyers['total_renewable_energy_consumption_wh'] = total_renewable_consumed_list
        df_kpi_buyers['share_renewables_consumption'] = share_renewable_consumed_list
    os.chdir(homepath_usecase)

    return df_kpi_buyers
