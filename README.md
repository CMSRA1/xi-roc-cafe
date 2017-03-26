# xi-roc-cafe
an example analysis code with alphatwirl and data frames

## a quick example

### check out

Log in to soolin.

Move to a work dir:
```bash
cd /some/work/dir/
```

The code runs in any envriomment with Python 2.7, ROOT 6, and several other common libraries.

In this example, we will run in `cmsenv`. 

Source `cmsset_default.sh` if it is not done yet.
```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
```

Check out `cmsenv` if it is not done yet.
```bash
export SCRAM_ARCH=slc6_amd64_gcc530
cmsrel CMSSW_8_0_26
```

Enter the `cmsenv`:
```bash
cd CMSSW_8_0_26/src/
cmsenv
cd ../../
```

Check out this repo:
```bash
git clone git@github.com:CMSRA1/xi-roc-cafe.git
```

Check out submodules:
```bash
cd xi-roc-cafe
git submodule init
git submodule update
cd ..
```

### run

Read trees for QCD
```bash
./xi-roc-cafe/twirl_mktbl_heppy.py --components QCD_HT100to200_madgraphMLM QCD_HT200to300_madgraphMLM QCD_HT300to500_madgraphMLM QCD_HT500to700_madgraphMLM QCD_HT700to1000_madgraphMLM QCD_HT1000to1500_madgraphMLM QCD_HT1500to2000_madgraphMLM QCD_HT2000toInf_madgraphMLM -i /hdfs/SUSY/RA1/80X/MC/20161021_B03/ROC_MC_SM -o ./tbl_001/QCD --parallel-mode htcondor --max-events-per-process 500000 --logging-level INFO
```

Read trees for T1tttt
```bash
./xi-roc-cafe/twirl_mktbl_heppy.py --components SMS_T1tttt_madgraphMLM -i /hdfs/SUSY/RA1/80X/MC/20161021_B04/ROC_MC_SMS -o ./tbl_001/T1tttt --parallel-mode htcondor --max-events-per-process 500000 --logging-level INFO --susy-sms
```

The above two commands create a set of data frames in the directory `tbl_001/`.

They also create a temporary dir `_ccsp_temp/`. Remove it if you don't need:
```bash
rm -rf _ccsp_temp/
```

The same data frames are in `example_tbl/tbl_twirl` in this repository.

Instead of watining for the jobs to finish, you can copy them and continue the example.
```bash
cp -a ./xi-roc-cafe/example_tbl/tbl_twirl ./tbl_002/
```

Copy a table with the relations between data sets and processes:
```bash
cp -a xi-roc-cafe/tbl/tbl_cfg_component_phasespace_process.txt tbl_002/QCD/
```

Analyze the data frames:
```bash
./xi-roc-cafe/tbl_roc.py --dir ./tbl_002/
```

This will print the ROC for two variables xi and minOmegaTilde against QCD and two mass points of T1tttt in bins of HT and njets.

## Links

* https://indico.cern.ch/event/590196/contributions/2380011/
* https://indico.cern.ch/event/384920/contributions/1813060/
* https://indico.cern.ch/event/388468/sessions/160219/#20150421
