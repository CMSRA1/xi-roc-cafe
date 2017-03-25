#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import argparse
import pandas as pd

import aggregate as ag

##__________________________________________________________________||
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 4096)
pd.set_option('display.max_rows', 65536)
pd.set_option('display.width', 1000)

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('--dir', default = 'tbl_002', help = 'path to the tbl dir')
args = parser.parse_args()

##__________________________________________________________________||
tbldir = args.dir

#
# load data frames
#
tbl_qcd_minOmegaTilde = ag.custom_pd_read_table(
    os.path.join(tbldir, 'QCD', 'tbl_n_component.htbin.njetbin.mhtbin.minOmegaTilde.txt'),
    dtype = dict(n = float, nvar = float)
)

# print tbl_qcd_minOmegaTilde.head()
# print tbl_qcd_minOmegaTilde.describe()
# print tbl_qcd_minOmegaTilde.dtypes
# print tbl_qcd_minOmegaTilde.component.cat.categories

tbl_qcd_xi = ag.custom_pd_read_table(
    os.path.join(tbldir, 'QCD', 'tbl_n_component.htbin.njetbin.mhtbin.xi.txt'),
    dtype = dict(n = float, nvar = float)
)

tbl_t1tttt_minOmegaTilde = ag.custom_pd_read_table(
    os.path.join(tbldir, 'T1tttt', 'tbl_n_component.smsmass1.smsmass2.htbin.njetbin.mhtbin.minOmegaTilde.txt'),
    dtype = dict(n = float, nvar = float)
)

tbl_t1tttt_xi = ag.custom_pd_read_table(
    os.path.join(tbldir, 'T1tttt', 'tbl_n_component.smsmass1.smsmass2.htbin.njetbin.mhtbin.xi.txt'),
    dtype = dict(n = float, nvar = float)
)

tbl_qcd_xsec = ag.custom_pd_read_table(
    os.path.join(tbldir, 'QCD', 'tbl_xsec.txt'),
    dtype = dict(xsec = float)
)

tbl_qcd_nevt = ag.custom_pd_read_table(
    os.path.join(tbldir, 'QCD', 'tbl_nevt.txt'),
    dtype = dict(nevt = float, nevt_sumw = float)
)

tbl_sm_component_process = ag.custom_pd_read_table(
    os.path.join(tbldir, 'QCD', 'tbl_cfg_component_phasespace_process.txt')
)

#
# merge or split components (data sets) to processes
#
tbl_qcd_minOmegaTilde = ag.combine_mc_components(
    tbl_qcd_minOmegaTilde, tbl_sm_component_process, tbl_qcd_nevt, tbl_qcd_xsec
)

tbl_qcd_xi = ag.combine_mc_components(
    tbl_qcd_xi, tbl_sm_component_process, tbl_qcd_nevt, tbl_qcd_xsec
)

tbl_t1tttt_minOmegaTilde = ag.split_component_for_smsmass(tbl_t1tttt_minOmegaTilde)

tbl_t1tttt_xi = ag.split_component_for_smsmass(tbl_t1tttt_xi)


#
# sum over MHT bins
#
tbl_qcd_minOmegaTilde = ag.sum_over_categories(
    tbl_qcd_minOmegaTilde, categories = ['mhtbin'], variables = ['n', 'nvar']
)

tbl_qcd_xi = ag.sum_over_categories(
    tbl_qcd_xi, categories = ['mhtbin'], variables = ['n', 'nvar']
)

tbl_t1tttt_minOmegaTilde = ag.sum_over_categories(
    tbl_t1tttt_minOmegaTilde, categories = ['mhtbin'], variables = ['n', 'nvar']
)

tbl_t1tttt_xi = ag.sum_over_categories(
    tbl_t1tttt_xi, categories = ['mhtbin'], variables = ['n', 'nvar']
)

#
# calculate cumulative distributions and selection efficiencies
#
tbl_qcd_minOmegaTilde = ag.calculate_efficiency(
    tbl_qcd_minOmegaTilde, varname = 'minOmegaTilde'
)

tbl_qcd_xi = ag.calculate_efficiency(
    tbl_qcd_xi, varname = 'xi'
)

tbl_t1tttt_minOmegaTilde = ag.calculate_efficiency(
    tbl_t1tttt_minOmegaTilde, varname = 'minOmegaTilde'
)

tbl_t1tttt_xi = ag.calculate_efficiency(
    tbl_t1tttt_xi, varname = 'xi'
)

#
# create ROCs
#
tbl_minOmegaTilde = ag.rbind_tbls([tbl_qcd_minOmegaTilde, tbl_t1tttt_minOmegaTilde])
tbl_minOmegaTilde = ag.create_roc(tbl_minOmegaTilde, process1 = ['QCD'], varname = 'minOmegaTilde')

tbl_xi = ag.rbind_tbls([tbl_qcd_xi, tbl_t1tttt_xi])
tbl_xi = ag.create_roc(tbl_xi, process1 = ['QCD'], varname = 'xi')

#
# bind into one table
#
tbl_minOmegaTilde = ag.gather_var(tbl_minOmegaTilde, varname = 'minOmegaTilde')
tbl_xi = ag.gather_var(tbl_xi, varname = 'xi')

tbl_roc = ag.rbind_tbls([tbl_minOmegaTilde, tbl_xi])

#
print tbl_roc.to_string(index = False)

##__________________________________________________________________||
