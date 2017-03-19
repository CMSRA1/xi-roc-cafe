# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
def split_component_for_smsmass(tbl_n_component):

    tbl_out = tbl_n_component.copy()

    tbl_out['process'] = tbl_out['component'].str.split('_').str[1] + '_' + tbl_out['smsmass1'].astype(str) + '_' + tbl_out['smsmass2'].astype(str)
    # e.g., 'T1tttt_1300_1050'

    del tbl_out['component']
    del tbl_out['smsmass1']
    del tbl_out['smsmass2']

    columns = ['process'] + [c for c in tbl_out.columns.values if c != 'process']
    tbl_out = tbl_out[columns]

    return tbl_out

##__________________________________________________________________||
