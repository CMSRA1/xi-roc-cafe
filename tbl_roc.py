#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import re
import argparse
import logging
import pandas as pd

sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'AlphaTwirl'))
import alphatwirl

sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'utils'))
from parallel import build_parallel
from profile_func import profile_func

import aggregate as ag

##__________________________________________________________________||
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 4096)
pd.set_option('display.max_rows', 65536)
pd.set_option('display.width', 1000)

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('--dir', default = 'tbl_002', help = 'path to the tbl dir')
parser.add_argument('--parallel-mode', default = 'multiprocessing', choices = ['multiprocessing', 'subprocess', 'htcondor'], help = "mode for concurrency")
parser.add_argument("-p", "--process", default = 8, type = int, help = "number of processes to run in parallel")
parser.add_argument('--profile', action = 'store_true', help = 'run profile')
parser.add_argument('--profile-out-path', default = None, help = 'path to write the result of profile')
parser.add_argument('--logging-level', default = 'WARN', choices = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help = 'level for logging')
args = parser.parse_args()

##__________________________________________________________________||
logging.basicConfig(level = logging.getLevelName(args.logging_level))

##__________________________________________________________________||
def main():

    #
    # load data frames
    #
    tbldir = args.dir

    tbl_qcd_minOmegaTilde = ag.custom_pd_read_table(
        os.path.join(tbldir, 'QCD', 'tbl_n_component.htbin.njetbin.mhtbin.minOmegaTilde.txt'),
        dtype = dict(n = float, nvar = float)
    )

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

    # print tbl_qcd_minOmegaTilde[0:10]
    # print tbl_qcd_minOmegaTilde.describe()
    # print tbl_qcd_minOmegaTilde.dtypes
    # print tbl_qcd_minOmegaTilde.component.cat.categories

    #
    #
    #
    tbl_qcd_minOmegaTilde = ag.combine_mc_components(
        tbl_qcd_minOmegaTilde, tbl_sm_component_process, tbl_qcd_nevt, tbl_qcd_xsec
    )

    tbl_qcd_xi = ag.combine_mc_components(
        tbl_qcd_xi, tbl_sm_component_process, tbl_qcd_nevt, tbl_qcd_xsec
    )

    tbl_t1tttt_minOmegaTilde =ag.split_component_for_smsmass(tbl_t1tttt_minOmegaTilde)

    tbl_t1tttt_xi =ag.split_component_for_smsmass(tbl_t1tttt_xi)

    #
    #
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
    #
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
    #
    #
    tbl_minOmegaTilde = ag.rbind_tbls([tbl_qcd_minOmegaTilde, tbl_t1tttt_minOmegaTilde])
    tbl_xi = ag.rbind_tbls([tbl_qcd_xi, tbl_t1tttt_xi])

    #
    #
    #
    tbl_minOmegaTilde = ag.create_roc(tbl_minOmegaTilde, process1 = ['QCD'], varname = 'minOmegaTilde')
    tbl_xi = ag.create_roc(tbl_xi, process1 = ['QCD'], varname = 'xi')

    #
    #
    #
    tbl_minOmegaTilde = ag.gather_var(tbl_minOmegaTilde, varname = 'minOmegaTilde')
    tbl_xi = ag.gather_var(tbl_xi, varname = 'xi')

    #
    #
    #
    tbl_roc = ag.rbind_tbls([tbl_minOmegaTilde, tbl_xi])

    print tbl_roc.to_string(index = False)

##__________________________________________________________________||
if __name__ == '__main__':
    if args.profile:
        profile_func(func = main, profile_out_path = args.profile_out_path)
    else:
        main()
