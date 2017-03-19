#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os
import re
import ast
import operator

##__________________________________________________________________||
def all_outputs_are_newer_than_any_input(outfile_paths, infile_paths):
    existing_infile_paths = [p for p in infile_paths if os.path.exists(p)]
    for outfile_path in outfile_paths:
        if not os.path.exists(outfile_path): return False
        for infile_path in existing_infile_paths:
            if os.path.getmtime(outfile_path) <= os.path.getmtime(infile_path): return False
    return True

##__________________________________________________________________||
def read_table_as_category_unless_specified(path, dtype = { }):
    """

    dtype : e.g., {'n': float, 'nvar': float}

    """
    import pandas as pd

    try:
        columns = open(path).readline().split()
    except TypeError: # path is buffer
        columns = path.readline().split()
        path.seek(0)
    # e.g, columns = ['component', 'cutflow', 'bintype', 'nJet100', 'n', 'nvar']

    columns_specified = dtype.keys()
    # e.g. ['nvar', 'n']

    str_columns = [c for c in columns if c not in columns_specified]
    # e.g, ['component', 'cutflow', 'bintype', 'nJet100']

    column_type_dict = dtype.copy()
    column_type_dict.update(dict([(c, str) for c in str_columns]))
    # e.g. {'component': <type 'str'>, 'nJet100': <type 'str'>, 'nvar': <type
    # 'float'>, 'cutflow': <type 'str'>, 'bintype': <type 'str'>, 'n': <type
    # 'float'>}

    tbl = pd.read_table(path, delim_whitespace = True, dtype = column_type_dict)

    return convert_column_types_to_category(tbl, str_columns)

##__________________________________________________________________||
def convert_column_types_to_category(tbl, columns):
    tbl = tbl.copy()

    ## http://pandas.pydata.org/pandas-docs/stable/categorical.html
    for c in columns:
        if not c in tbl.columns: continue

        tbl[c] = tbl[:][c].astype('category', ordered = True)
        ## not clear why.  but without '[:]', sometime 'ordered' is not effective

        try:
            ## order numerically if numeric
            categories = [(e, ast.literal_eval(str(e))) for e in tbl[c].cat.categories]
            # e.g., [('100.47', 100.47), ('15.92', 15.92), ('2.0', 2.0)]

            categories = sorted(categories, key = operator.itemgetter(1))
            # e.g., [('2.0', 2.0), ('15.92', 15.92), ('100.47', 100.47)]

            categories = [e[0] for e in categories]
            # e.g., ['2.0', '15.92', '100.47']

        except:
            ## alphanumeric sort
            categories = sorted(tbl[c].cat.categories, key = lambda n: [float(c) if c.isdigit() else c for c in re.findall('\d+|\D+', n)])

        tbl[c].cat.reorder_categories(categories, ordered = True, inplace = True)

    return tbl

##__________________________________________________________________||
def copy_dtype_category(dest, src):
    ret = dest.copy()
    for col in dest.columns:
        if not col in src.columns: continue
        if dest[col].dtype is src[col].dtype: continue
        if not src[col].dtype.name == 'category': continue
        ret[col] = ret[col].astype('category',
                                    categories = src[col].cat.categories,
                                    ordered = src[col].cat.ordered)
    return ret

##__________________________________________________________________||
def write_to_file(tbl, path):
    print 'writing ', path
    f = open(path, 'w')
    tbl.to_string(f, index = False)
    f.write("\n")
    f.close()

##__________________________________________________________________||
