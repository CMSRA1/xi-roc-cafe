# Tai Sakuma <tai.sakuma@cern.ch>
import itertools
import numpy as np
import pandas as pd

from .dtype import keep_dtype

##__________________________________________________________________||
def create_roc(tbl_in, process1, varname):

    process1 = [p for p in process1 if p in tbl_in['process'].cat.categories]
    # e.g., ['QCD', 'EWK', 'SM']

    variables = (varname, 'process', 'n', 'nvar', 'cumn', 'eff', 'luminosity')
    # e.g., varname = 'minChi'

    factor_names = [c for c in tbl_in.columns if c not in variables]
    # e.g., ['htbin', 'mhtbin']

    tbl_in = tbl_in[factor_names + ['process', varname, 'eff', 'cumn']]

    tbl_process1 = tbl_in[tbl_in['process'].isin(process1)].copy()
    tbl_process1.rename(columns = {'process': 'process1', 'eff': 'eff1', 'cumn': 'cumn1'}, inplace = True)

    tbl_process2 = tbl_in.copy()
    tbl_process2.rename(columns = {'process': 'process2', 'eff': 'eff2', 'cumn': 'cumn2'}, inplace = True)

    # fill missing values
    tbl_mesh = pd.DataFrame(list(itertools.product(*([tbl_process1[varname].unique()] + [tbl_process2[f].unique() for f in factor_names + ['process2']]))))
    tbl_mesh.columns = [varname] + factor_names + ['process2']
    tbl_process2 =  pd.merge(tbl_mesh, tbl_process2, how = 'left')

    tbl_out = pd.merge(tbl_process1, tbl_process2)

    tbl_out = tbl_out[tbl_out['process1'] != tbl_out['process2']]

    tbl_out = tbl_out[factor_names + ['process1', 'process2', varname, 'eff1', 'eff2', 'cumn1', 'cumn2']]

    tbl_out = tbl_out.groupby(factor_names + ['process1', 'process2']).apply(pad_na).reset_index(drop = True)

    #
    tbl_out = keep_dtype(tbl_out, tbl_in)

    # sort
    tbl_out['process1'] = tbl_out['process1'].astype(
        'category',
        categories = process1,
        ordered = True
    )
    tbl_out['process2'] = tbl_out['process2'].astype(
        'category',
        categories = tbl_in['process'].cat.categories,
        ordered = tbl_in['process'].cat.ordered
    )
    tbl_out = tbl_out.sort_values(['process1'] + factor_names + ['process2', 'eff1', 'eff2'])

    tbl_out = tbl_out.reset_index(drop = True)

    return tbl_out

##__________________________________________________________________||
def pad_na(tbl):
    if tbl.empty: return tbl
    tbl = tbl.sort_values(['eff1', 'eff2'])
    if np.isnan(tbl['eff2'].iloc[0]): tbl['eff2'].iloc[0] = 0
    if np.isnan(tbl['cumn2'].iloc[0]): tbl['cumn2'].iloc[0] = 0
    tbl.fillna(method = 'pad', inplace = True)
    return tbl

##__________________________________________________________________||
