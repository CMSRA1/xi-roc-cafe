#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import argparse
import logging
import pprint

##__________________________________________________________________||
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'AlphaTwirl'))
import alphatwirl

##__________________________________________________________________||
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'utils'))
import framework_heppy

##__________________________________________________________________||
default_heppydir = '/hdfs/SUSY/RA1/80X/MC/20161021_B03/ROC_MC_SM'

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument("--mc", action = "store_const", dest = 'datamc', const = 'mc', default = 'mc', help = "for processing MC")
parser.add_argument("--data", action = "store_const", dest = 'datamc', const = 'data', help = "for processing data")
parser.add_argument('-i', '--heppydir', default = default_heppydir, help = 'Heppy results dir')
parser.add_argument('-c', '--components', default = None, nargs = '*', help = 'the list of components')
parser.add_argument('-o', '--outdir', default = os.path.join('tbl', 'out'))
parser.add_argument('--susy-sms', action = 'store_true', default = False, help = 'whether running on SUSY SMS')
parser.add_argument('-n', '--nevents', default = -1, type = int, help = 'maximum number of events to process for each component')
parser.add_argument('--max-events-per-process', default = -1, type = int, help = 'maximum number of events per process')
parser.add_argument('--force', action = 'store_true', default = False, dest='force', help = 'recreate all output files')

parser.add_argument('--parallel-mode', default = 'multiprocessing', choices = ['multiprocessing', 'subprocess', 'htcondor'], help = "mode for concurrency")
parser.add_argument('-p', '--processes', default = 4, type = int, help = 'number of processes to run in parallel')
parser.add_argument('-q', '--quiet', default = False, action = 'store_true', help = 'quiet mode')
parser.add_argument('--profile', action = "store_true", help = "run profile")
parser.add_argument('--profile-out-path', default = None, help = "path to write the result of profile")
parser.add_argument('--logging-level', default = 'WARN', choices = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help = "level for logging")

args = parser.parse_args()

##__________________________________________________________________||
def main():

    #
    # configure logger
    #
    log_level = logging.getLevelName(args.logging_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    names_for_logger = ["framework_heppy", "alphatwirl"]
    for n in names_for_logger:
        logger = logging.getLogger(n)
        logger.setLevel(log_level)
        logger.handlers[:] = [ ]
        logger.addHandler(log_handler)

    #
    #
    #
    reader_collector_pairs = [ ]

    #
    # configure scribblers
    #
    from atlogic.Scribblers.componentName import componentName
    from atlogic.Scribblers.MhtOverMet import MhtOverMet
    from atlogic.Scribblers.cutflow import cutflow
    from atlogic.Scribblers.multiply import multiply
    scribblers = [
        componentName(),
        MhtOverMet(),
        cutflow(),
    ]

    from scribblers.SMSMass import SMSMass
    scribblers_SMS = [
        SMSMass(),
    ]

    if args.susy_sms:
        scribblers.extend(scribblers_SMS)

    reader_collector_pairs.extend([(r, alphatwirl.loop.NullCollector()) for r in scribblers])

    #
    # configure tables (before the event selection)
    #


    #
    # configure event selections
    #
    path_cfg_common = dict(All = (
        'ev : ev.cutflowId[0] == 1',
        'ev : ev.nIsoTracksVeto[0] <= 0',
        'ev : ev.nJet40[0] >= 2',
        'ev : ev.ht40[0] >= 200',
        'ev : ev.nJet100[0] >= 1',
        'ev : ev.nJet40failedId[0] == 0',
        'ev : ev.nJet40Fwd[0] == 0',
        'ev : -2.5 < ev.jet_eta[0] < 2.5',
        'ev : 0.1 <= ev.jet_chHEF[0] < 0.95',
        'ev : 130 <= ev.mht40_pt[0]',
        'ev : ev.MhtOverMet[0] < 1.25',
    ))

    path_cfg_susy_masspoints = dict(Any = (
        dict(All = (
            'ev : ev.componentName[0] == "SMS_T1tttt_madgraphMLM"',
            dict(Any = (
                dict(All = ('ev : ev.smsmass1[0] == 1300', 'ev : ev.smsmass2[0] == 1050', path_cfg_common)),
                dict(All = ('ev : ev.smsmass1[0] == 1800', 'ev : ev.smsmass2[0] == 500', path_cfg_common)),
            )),
        )),
        dict(All = (
            'ev : ev.componentName[0] == "SMS_T2bb_madgraphMLM"',
            dict(Any = (
                dict(All = ('ev : ev.smsmass1[0] == 500', 'ev : ev.smsmass2[0] == 450', path_cfg_common)),
                dict(All = ('ev : ev.smsmass1[0] == 1000', 'ev : ev.smsmass2[0] == 300', path_cfg_common)),
            )),
        )),
    ))

    path_cfg = path_cfg_common
    if args.susy_sms:
        path_cfg = path_cfg_susy_masspoints

    eventselection_path = os.path.join(args.outdir, 'eventselection.txt')
    if args.force or not os.path.exists(eventselection_path):
        alphatwirl.mkdir_p(os.path.dirname(eventselection_path))
        with open(eventselection_path, 'w') as f:
            pprint.pprint(path_cfg, stream = f)

    from atlogic.buildEventSelection import buildEventSelection

    tbl_cutflow_path = os.path.join(args.outdir, 'tbl_cutflow.txt')
    if args.force or not os.path.exists(tbl_cutflow_path):
        from atlogic.EventSelectionModules.EventSelectionAllCount import EventSelectionAllCount
        from atlogic.EventSelectionModules.EventSelectionAnyCount import EventSelectionAnyCount
        from atlogic.EventSelectionModules.EventSelectionNotCount import EventSelectionNotCount

        eventSelection = buildEventSelection(
            path_cfg = path_cfg,
            AllClass = EventSelectionAllCount,
            AnyClass = EventSelectionAnyCount,
            NotClass = EventSelectionNotCount
        )

        resultsCombinationMethod = alphatwirl.collector.CombineIntoList(
            summaryColumnNames = ('depth', 'class', 'name', 'pass', 'total'),
            sort = False,
            summarizer_to_tuple_list = summarizer_to_tuple_list
        )
        deliveryMethod = alphatwirl.collector.WriteListToFile(tbl_cutflow_path)
        collector = alphatwirl.loop.Collector(resultsCombinationMethod, deliveryMethod)
    else:
        eventSelection = buildEventSelection(
            path_cfg = path_cfg
        )
        collector = alphatwirl.loop.NullCollector()

    reader_collector_pairs.append((eventSelection, collector))

    #
    # configure tables (after the event selection)
    #
    Binning = alphatwirl.binning.Binning
    Echo = alphatwirl.binning.Echo
    Round = alphatwirl.binning.Round
    RoundLog = alphatwirl.binning.RoundLog
    echo = Echo(nextFunc = None)
    echoNextPlusOne = Echo()

    tableConfigCompleter = alphatwirl.configure.TableConfigCompleter(
        defaultSummaryClass = alphatwirl.summary.Count,
        defaultOutDir = args.outdir,
        createOutFileName = alphatwirl.configure.TableFileNameComposer2()
        )

    njetbin = Binning(boundaries = (1, 2, 3, 6, 9))
    htbin = Binning(boundaries = (0, 200, 400, 800, 1200))
    mhtbin = Binning(boundaries = (0, 130, 200, 400, 800))

    tblcfg = [
        dict(
            keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'minOmegaTilde'),
            keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'minOmegaTilde'),
            binnings = (htbin, njetbin, mhtbin, Round(0.05, 0, min = 0))
        ),
        dict(
            keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'xi'),
            keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'xi'),
            binnings = (htbin, njetbin, mhtbin, Round(0.05, 0, min = 0))
        ),
    ]

    if args.susy_sms:
        for c in tblcfg:
            keyOutColumnNames = c['keyOutColumnNames'] if 'keyOutColumnNames' in c else c['keyAttrNames']
            c['keyAttrNames'] = ('smsmass1', 'smsmass2') + c['keyAttrNames']
            c['binnings'] = (echo, echo) + c['binnings']
            if 'keyIndices' in c: c['keyIndices'] = (None, None) + c['keyIndices']
            c['keyOutColumnNames'] = ('smsmass1', 'smsmass2') + keyOutColumnNames

    tblcfg = [tableConfigCompleter.complete(c) for c in tblcfg]
    if not args.force:
        tblcfg = [c for c in tblcfg if c['outFile'] and not os.path.exists(c['outFilePath'])]

    reader_collector_pairs.extend([alphatwirl.configure.build_counter_collector_pair(c) for c in tblcfg])

    #
    # run
    #
    fw =  framework_heppy.FrameworkHeppy(
        outdir = args.outdir,
        heppydir = args.heppydir,
        datamc = args.datamc,
        force = args.force,
        quiet = args.quiet,
        parallel_mode = args.parallel_mode,
        process = args.processes,
        user_modules = ('atlogic', 'scribblers'),
        max_events_per_dataset = args.nevents,
        max_events_per_process = args.max_events_per_process,
        profile = args.profile,
        profile_out_path = args.profile_out_path

    )
    fw.run(
        components = args.components,
        reader_collector_pairs = reader_collector_pairs
    )

##__________________________________________________________________||
def summarizer_to_tuple_list(summarizer, sort):
    return [tuple(e) for e in summarizer._results]

##__________________________________________________________________||
if __name__ == '__main__':
    main()
