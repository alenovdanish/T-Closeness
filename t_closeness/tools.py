# tools.py


import hashlib
import pandas as pd




class ErrAnony(Exception):
    def __init__(self, msg):
        self.msg = msg



def get_chunks(dFrame, categoryData, subdivide, scale=None):

    cols = list(dFrame.columns)
    chunks = {}
    for col in dFrame.columns:
        if col in categoryData:
            span = len(dFrame[col][subdivide].unique())
        else:
            span = dFrame[col][subdivide].max()-dFrame[col][subdivide].min()
        if scale is not None:
            span = span/scale[col]
        chunks[col] = span
    return chunks


def get_whole_chunks(dFrame, categoryData):
    for name in dFrame.columns:
        if name not in categoryData:
            dFrame[name] = pd.to_numeric(dFrame[name])

    return get_chunks(dFrame, categoryData, dFrame.index)



def split(dFrame, categoryData, subdivide, col):
    dpPart = dFrame[col][subdivide]
    if col in categoryData:
        vals = dpPart.unique()
        leftValue = set(vals[:len(vals)//2])
        rightValue = set(vals[len(vals)//2:])
        return dpPart.index[dpPart.isin(leftValue)], dpPart.index[dpPart.isin(rightValue)]
    else:
        median = dpPart.median()
        dfl = dpPart.index[dpPart < median]
        dfr = dpPart.index[dpPart >= median]
        return (dfl, dfr)


def is_k_anony(subdivide, k):

    if len(subdivide) < k:
        return False
    return True





def t_closeness(dFrame, subdivide, col, global_freqs):
    total_cnt = float(len(subdivide))
    d_max = None
    group_cnt = dFrame.loc[subdivide].groupby(col)[col].agg('count')
    for value, count in group_cnt.to_dict().items():
        p = count/total_cnt
        d = abs(p-global_freqs[value])
        if d_max is None or d > d_max:
            d_max = d
    return d_max




def is_t_close(dFrame, subdivide, categoryData, col_sensitive, global_freqs, p):

    if not col_sensitive in categoryData:
        raise ValueError("This (T-Closeness) only works with with the data having categorical type.")
    result = t_closeness(dFrame, subdivide, col_sensitive, global_freqs) <= p
    if(result):
        return result
    else:
        print("T Closeness : False")


def get_global_freq(dFrame, col_sensitive):
    global_freqs = {}
    total_cnt = float(len(dFrame))
    group_cnt = dFrame.groupby(col_sensitive)[col_sensitive].agg('count')

    for value, count in group_cnt.to_dict().items():
        p = count/total_cnt
        global_freqs[value] = p
    return global_freqs



def bifurcate_dataset(dFrame, k, l, t, categoryData, col_features, col_sensitive, scale):

    final_part = []
    global_freqs = {}
    if t is not None:
        global_freqs = get_global_freq(dFrame, col_sensitive)

    subdivides = [dFrame.index]
    while subdivides:
        subdivide = subdivides.pop(0)
        chunks = get_chunks(dFrame[col_features],
                          categoryData, subdivide, scale)
        for col, span in sorted(chunks.items(), key=lambda x: -x[1]):
            leftPart, rightPart = split(dFrame, categoryData, subdivide, col)
            if l is not None:
                if not is_k_anony(leftPart, k) or not is_k_anony(rightPart, k) or not is_l_diverse(dFrame, leftPart, col_sensitive, l) or not is_l_diverse(dFrame, rightPart, col_sensitive, l):
                    continue
            if l is None:
                if t is None:
                    if not is_k_anony(leftPart, k) or not is_k_anony(rightPart, k):
                        continue
                if t is not None:
                    if not is_k_anony(leftPart, k) or not is_k_anony(rightPart, k) or not is_t_close(dFrame, leftPart, categoryData, col_sensitive, global_freqs, t) or not is_t_close(dFrame, rightPart, categoryData, col_sensitive, global_freqs, t):
                        continue
            subdivides.extend((leftPart, rightPart))
            break
        else:
            final_part.append(subdivide)
    return final_part


def agg_categoryData_col(series):
    vals = set()
    for value in series:
      print(value)
      if value is not None:
        vals.add(str(value))
    return ','.join(set(series))


def agg_num_col(series):
    minimum = series.min()
    maximum = series.max()
    if(maximum == minimum):
        string = str(maximum)
    else:
        string = ''
        maxm = str(maximum)
        minm = str(minimum)

        if(len(minm) == 1):
            if(minimum >= 5):
                string = '5-'
            else:
                string = '0-'
        else:
            if (minm[-1]=='0'):
                string = minm +"-"
            else:
                min_start = minm[:-1]
                if(minimum >= int(min_start+'5')):
                    string = min_start+'5-'
                else:
                    string = min_start+'0-'

        if(len(maxm) == 1):
            if(maximum >= 5):
                string += "10"
            else:
                string += '5'
        else:
            if(maxm[-1]=='0'):
                string += maxm
            else:
                max_start = maxm[:-1]
                if(maximum > int(max_start+'5')):
                    string += str(int(max_start+'0') + 10)
                else:
                    string += max_start+'5'

    return string

def t_anony(dFrame, subdivides, col_features, col_sensitive, categoryData, max_subdivides=None):
    accumulations = {}

    for col in col_features:
        if col in categoryData:
            accumulations[col] = agg_categoryData_col
        else:
            accumulations[col] = agg_num_col
    rows = []

    for i, subdivide in enumerate(subdivides):
        if i % 100 == 1:
            print("Processing Done {} subdivides.".format(i))
        if max_subdivides is not None and i > max_subdivides:
            break
        collective_col = dFrame.loc[subdivide].assign(
            _common88column_=1).groupby('_common88column_').agg(accumulations, squeeze=False)
        sensitive_cnts = dFrame.loc[subdivide].groupby(
            col_sensitive).agg({col_sensitive: 'count'})
        vals = collective_col.iloc[0].to_dict()
        for sensitive_value, count in sensitive_cnts[col_sensitive].items():
            if count == 0:
                continue
            vals.update({
                col_sensitive: sensitive_value,
                'count': count,

            })
            rows.append(vals.copy())
    fin = pd.DataFrame(rows)
    pfin = fin.sort_values(col_features+[col_sensitive])
    return pfin