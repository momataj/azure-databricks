"""
Case insenstive columns
Will returns the actual column case
Support the existing datatypes
Default value can be customizable
Pass multiple dataframes at once (e.g unionAll(df1, df2, df3, ..., df10))
"""
import functools
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def unionAll(dfs, fill_by='N/A'):
    clmns = {clm.name.lower(): (clm.dataType, clm.name) for df in dfs for clm in df.schema.fields}
    print(clmns)
    
    for i, df in enumerate(dfs):
        
        df_clmns = [clm.lower() for clm in df.columns]
        for clm, (dataType, name) in clmns.items():
            if clm not in df_clmns:
                # Add the missing column
                dfs[i] = dfs[i].withColumn(name, F.lit(fill_by).cast(dataType))
    return reduce(DataFrame.unionByName, dfs)
  
  
  
  # Execute function
  
dfs=[df1,df2,df3,df5,......,dfn]
df_unions=unionAll(dfs)
