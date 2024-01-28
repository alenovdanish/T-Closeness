
from pyspark.sql.functions import PandasUDFType, lit, pandas_udf
from t_closeness.tools import *

def t_closeness_anonymizer(df, k, t, col_feature, col_sensitive, categoryData):

    if col_sensitive not in df.columns:
        raise ErrAnony("No Such Sensitive Column")

    for fcol in col_feature:
        if fcol not in df.columns:
            raise ErrAnony("No Such Feature col :"+fcol)

    whole_chunks = get_whole_chunks(df, categoryData)
    subdivides = bifurcate_dataset(
        df, k, None, t,  categoryData, col_feature, col_sensitive, whole_chunks)

    return t_anony(df, subdivides, col_feature, col_sensitive, categoryData)

class Preserver:

    @staticmethod
    def t_closeness(df, kVal, thresold, col_feature, col_sensitive, categoryData, schema):

        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def anonymize(pdf):
            a_df = t_closeness_anonymizer(pdf, kVal, thresold, col_feature,
                                          col_sensitive, categoryData)
            return a_df

        return df.groupby().apply(anonymize)



