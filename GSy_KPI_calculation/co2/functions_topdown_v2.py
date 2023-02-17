import os
import pandas as pd

def get_use_case_dir_and_nr(home_path, use_case_nr):
    # check use case number for validity
    use_case_nr = str(use_case_nr)
    assert use_case_nr in ['0','1','2','2-1','2-2','3','3_v2','4','5','6'], f'ERROR: uc_nr is {use_case_nr}, must be in [0,1,2,2-1,2-2,3,4,5,6]'
    if use_case_nr == '0' and any('base' in x for x in [i.name for i in os.scandir(home_path) if i.is_dir()]):
        use_case_dir = [i.path for i in os.scandir(home_path) if i.is_dir() if 'base' in i.name.lower() if 'case' in i.name.lower()][0]
    else:
        use_case_dir = [i.path for i in os.scandir(home_path) if i.is_dir() if f'case_{use_case_nr}' in i.name.lower()][0]
    # treat versions of use case like original use case (3_v2 -> 3, 2-2 -> 2, etc.)
    if '-' in use_case_nr or '_' in use_case_nr or 'v' in use_case_nr:
        use_case_nr = use_case_nr[0]
    
    return use_case_dir, int(use_case_nr)


def prepare_df_out(use_case_dir):
    # obtain all months' time slots from .json files and combine them to index
    json_filepaths, timeslots, timeslots_len = [], [], []
    for root, dir, file in os.walk(top=use_case_dir, topdown=True):
        json_filepaths += [os.path.join(root, f) for f in file if 'price_energy_day.json' in f]
    for j in json_filepaths:
        df = pd.read_json(j)
        timeslots_j = [df['Germany']['price-energy-day'][i][0]['time'] for i in
                      range(len(df['Germany']['price-energy-day']))]
        timeslots += timeslots_j
        timeslots_len.append(len(timeslots_j))
    
    # verify that all months have an equal amount of timeslots, i.e. no data is missing
    assert timeslots_len.count(timeslots_len[0]) == len(timeslots_len), f'The number of timeslots in the price-energy-day.json file varies between the months, i.e. some data is missing. Please check file at e.g. {json_filepaths[0]} for each month.'
    
    # create dataframe with chronologically ordered timeslots
    timeslots.sort()
    df_out = pd.DataFrame(index=timeslots)
    
    return df_out


def prepare_empty_and_nonempty_filepaths(use_case_dir, use_case_nr):
    # get all filepaths and -names
    all_filepaths, all_filenames = [], []
    for root, dir, file in os.walk(top=use_case_dir, topdown=True):
        all_filepaths += [os.path.join(root, f) for f in file if 'trades.csv' in f]
        all_filenames += [f for f in file if 'trades.csv' in f]
    
    # depending on the use case, split files into empty and nonempty ones
    nonempty_files_paths, empty_files_paths = [], []
    # base case, use case 1, 4, and 5
    if use_case_nr < 2 or use_case_nr == 5:
        nonempty_files_paths = [i for i in all_filepaths if any(x in i.split('\\')[-1] for x in ('member', 'house'))]
        empty_files_paths = list(set(all_filepaths) - set(nonempty_files_paths))
    # use case 2 and 6
    elif use_case_nr == 2 or use_case_nr == 6:
        nonempty_files_paths = all_filepaths
    # use case 3
    elif use_case_nr == 3:
        nonempty_files_paths = [i for i in all_filepaths if any(x in i.split('\\')[-1] for x in ('member', 'region'))]
        empty_files_paths = list(set(all_filepaths) - set(nonempty_files_paths))
    # use case 4
    elif use_case_nr == 4:
        nonempty_files_paths = [i for i in all_filepaths if
                                any(x in i.split('\\')[-1] for x in ('member', 'house', 'ec'))]
        empty_files_paths = list(set(all_filepaths) - set(nonempty_files_paths))
    else:
        print('no use case found in use_case_home_path')
      
    months = all_filenames.count(all_filenames[0])
    nr_nonempty_files = len(nonempty_files_paths)
    assert nr_nonempty_files % months == 0, f'number of nonempty files ({nr_nonempty_files}) % number of months ({months}) is not zero. Uneven number of files.'
    
    return nonempty_files_paths, nr_nonempty_files, empty_files_paths
        

# creates separate dfs an entity's trading partners (p_to_c, c_to_p, etc.)
def share_renewable_df_grouper(df_out, df):
    df.index = [i.split(':00+')[0] for i in df.index.tolist()]
    df = df.groupby(df.index)[['energy [kWh]']].sum()
    df = df.reindex(df_out.index, fill_value=0)

    return df


def get_market_maker_share_renewables(months, use_case_nr, nonempty_files_paths, df_out):
    if use_case_nr != 2 and use_case_nr != 6:
        mm_filepaths = [i for i in nonempty_files_paths if 'member' in i.split('\\')[-1]]
    else:
        mm_filepaths = [i for i in nonempty_files_paths if 'germany-trades.csv' in i]
    nr_mm_filepaths = len(mm_filepaths)
    
    # set up df_mm
    df_mm = df_out.copy()
    df_mm[['mm_to_cs', 'cs_to_mm']] = 0, 0
    
    # for each entity involved with mm, combine all months' data frames
    entity_dfs = []
    for i in range(int(nr_mm_filepaths / months)):
        entity_monthly_dfs = []
        entity_monthly_filepaths = mm_filepaths[i::int(nr_mm_filepaths / months)]
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
        # c = 'germany' if 'grid' in children else children[0]
        c = children[0]
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
            if f'{p}_grey_energy [%]' in df_out.columns.tolist():
                df_out_copy.loc[
                    (df_out_copy[f'{c_name}_net_p_to_c'] > 0) & (df_out_copy[f'{c_name}_net_cs_to_c'] <= 0), [c_name]] = \
                    df_out[f'{p}_grey_energy [%]']
                p_share_grey_electricity = df_out[f'{p}_grey_energy [%]']
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
    if use_case_nr in [2, 6]:
        return df_out
    if use_case_nr in [0, 1, 5]:
        # ecs
        for reg in range(1, 7):
            for ec in range(6):
                df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg, ec)
        # regio
        for reg in range(1, 7):
            df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg, ec=None)
        # germany
        df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg=None, ec=None)
    if use_case_nr == 3:
        # germany
        df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg=None, ec=None)
    if use_case_nr == 4:
        # regio
        for reg in range(1, 7):
            df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg, ec=None)
        # germany
        df_out = share_renewables_aggregate_empty_entities_helper(df_out, reg=None, ec=None)

    return df_out


def co2_calculation(home_path, use_case_nr):
    months = 4
    
    # get directory path and number (int) for selected use case
    use_case_dir, use_case_nr = get_use_case_dir_and_nr(home_path, use_case_nr)
    
    # get time-indexed dataframe and all relevant filepaths
    df_out = prepare_df_out(use_case_dir)
    nonempty_files_paths, nr_nonempty_files, empty_files_paths = prepare_empty_and_nonempty_filepaths(use_case_dir, use_case_nr)
    
    # calculate net renewable share for market maker over all entities that trade with it
    df_mm = get_market_maker_share_renewables(months, use_case_nr, nonempty_files_paths, df_out)
    
    # combine each entity's monthly data frames and hand them over to shares_renewable_helper for calculation
    for i in range(int(nr_nonempty_files / months)):
        entity_monthly_dfs = []
        entity_monthly_filepaths = nonempty_files_paths[i::int(nr_nonempty_files / months)]
        for j in entity_monthly_filepaths:
            df_temp = pd.read_csv(j).drop(['creation_time', 'matching_requirements', 'rate [ct./kWh]'], axis=1)
            df_temp.seller = [i.lower().replace('_', '-') for i in df_temp.seller]
            df_temp.buyer = [i.lower().replace('_', '-') for i in df_temp.buyer]
            entity_monthly_dfs.append(df_temp)
        entity_name = entity_monthly_filepaths[0].split('\\')[-1].split('-trades.csv')[0]
        entity_df = pd.concat(entity_monthly_dfs).sort_values(by='slot').reset_index(drop=True)
        entity_df.set_index(['slot'], inplace=True)
        # hand over entity_df to helper function which inserts entity's result as new column into df_out
        df_out = share_renewable_helper(df_out, df_mm, entity_df, entity_name, co2=True)
    # aggregate sub-entities to fill missing top-level entities' values (depends on use case)
    df_out = share_renewables_aggregate_empty_entities(df_out, use_case_nr)
    
    # cosmetic change: rename 'grid' columns with 'germany'
    df_out.columns = [i.replace('grid', 'germany') if 'grid' in i else i for i in df_out.columns]

    return df_out