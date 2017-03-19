# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
def calculate_efficiency(tbl_in, varname, reverse = False):

    variables = (varname, 'n', 'nvar', 'luminosity')
    # e.g., ('minChi', 'n', 'nvar', 'luminosity')

    factor_names = [c for c in tbl_in.columns if c not in variables]
    # e.g., ['process', 'htbin', 'njetbin', 'mhtbin']

    tbl_out = tbl_in.groupby(factor_names).apply(add_cumn_eff, reverse = reverse).reset_index(drop = True)
    tbl_out = tbl_out.groupby(factor_names).apply(remove_last_extra_zero_rows).reset_index(drop = True)
    tbl_out = tbl_out.groupby(factor_names).apply(remove_first_extra_one_rows).reset_index(drop = True)

    ## columns =  factor_names + [varname, 'eff']
    ## tbl_out = tbl_out[columns]

    return tbl_out

##__________________________________________________________________||
def add_cumn_eff(tbl, reverse):
    if reverse:
        tbl = tbl[::-1]
    tbl['cumn'] = tbl[::-1]['n'].cumsum()
    sumn = sum(tbl['n'])
    tbl['eff'] = tbl['cumn']/sumn if sumn > 0 else 0
    return tbl

##__________________________________________________________________||
def remove_last_extra_zero_rows(tbl):
    if len(tbl.index) <= 1: return tbl
    n_zero_eff_rows = sum(tbl['eff'] == 0)
    if n_zero_eff_rows <= 1: return tbl
    return tbl.iloc[:(len(tbl.index) - n_zero_eff_rows + 1)]

##__________________________________________________________________||
def remove_first_extra_one_rows(tbl):
    if len(tbl.index) <= 1: return tbl
    n_one_eff_rows = sum(tbl['eff'] >= 1)
    if n_one_eff_rows <= 1: return tbl
    tbl = tbl.iloc[(n_one_eff_rows - 1):]
    return tbl

##__________________________________________________________________||
