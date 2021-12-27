#Setup Notebook Parameter
#dbutils.widgets.text("FileType", "")  #THIS IS THE EXPORT FOLDER THAT THE FILES WILL BE EXPORTED TO
pFileType = dbutils.widgets.get("FileType")
print(pFileType)

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

import pyspark.sql.functions as F
from datetime import date,datetime,timedelta

# Get the DateRange 
"""
UTC timestamp 
Yesterday= today -(days=1)
"""

fmt="%Y-%m-%dT%H:%M:%SZ"
today = date.today()
print(f"UTC TIME NOW:{today}")
UTCdatefrom = datetime.combine(today - timedelta(days = 1), datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
print(f'UTCdatefrom: {UTCdatefrom}')
UTCdateto = datetime.combine(today - timedelta(days = 1), datetime.max.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
print(f'UTCdateto: {UTCdateto}')

#Function: API data Ingestion (Json Data)

"""
API Request send to URL with Key for authentication
Response: Json format
"""
 
import requests
import json
import pandas as pd
from json.decoder import JSONDecodeError
 
def api(countryID,UTCdatefrom,UTCdateto):
   apikey ='*************************'
  print(f'countryID: {countryID}')
  url=f'https://api.*****************************/{countryID}/range/{UTCdatefrom}/{UTCdateto}'
  #print(url)
  payload = {'apikey': f'{apikey}'}
  response=requests.get(url=url,headers=payload )
  #print(response.headers["content-type"])
  response.raise_for_status()  
  if (
      response.status_code != 204 and  response.headers["content-type"].strip().startswith("application/json")
  ):
      try:
          return response.json()
      except requests.exceptions.HTTPError as e:
        print (e.response.text)
    
        
#Function:Get the schema of the dataframe

"""
Creating a schema 
Columns strcture as StringType
"""
from pyspark.sql.types import *
def creating_schema(df):
    sf = []
    for columnName in df.columns:
        sf.append(StructField(columnName, StringType(), True))
    return StructType(sf)


  
# Function:Combine Spark dataframe lists ( with common and uncommon columns)
  
  """
Case insenstive columns
Will returns the actual column case
Support the existing datatypes
Default value can be customizable
Pass multiple dataframes at once (e.g unionAll(df1, df2, df3, ..., df10))
"""
 
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
 
def unionAll(dfs, fill_by=None):
    clmns = {clm.name.lower(): (clm.dataType, clm.name) for df in dfs for clm in df.schema.fields}
    
    for i, df in enumerate(dfs):
        
        df_clmns = [clm.lower() for clm in df.columns]
        for clm, (dataType, name) in clmns.items():
            if clm not in df_clmns:
                # Add the missing column
                dfs[i] = dfs[i].withColumn(name, F.lit(fill_by).cast(dataType))
    return reduce(DataFrame.unionByName, dfs)

#Function: Handling missing columns in the dataframe
from pyspark.sql.functions import *
def detect_column(column, df, data_type):
          if not column in df.columns:
            ret = lit("").cast(data_type)
          else:
            ret = col(column).cast(data_type)            
          return ret
    
 
#main

import pandas as pd
from functools import reduce

# json_normalize and create pandas dataframe from Json Data 
def main():  

  countryIDList=["USA", "UK", "NZ", "FRANCE","AUSTRALIA", "JAPAN", "INDIA","BANGLADESH","SRILANKA"]
  dfs = []
  for countryID in (countryIDList): 
    # Call API function
    json_data=api(countryID=countryID,UTCdatefrom=UTCdatefrom,UTCdateto=UTCdateto)
    if len(json_data["results"]):  
      # normalize results and the preceding keys
      dfs.append(pd.json_normalize(json_data,'results', meta=['path', 'request-time', 'version',  ['attributes', 'dataset'],['attributes', 'provider'],['attributes', 'country'],['attributes', 'country-alpha']])) 
    else:
      print(f"no result for countryID :{countryID}")
  print(f'Total dfs:{len(dfs)}')
 
 #Union all dataframe using unionAll Functions
  
    """
  Create Spark dfs list from pandas dfs list
  Union All spark dfs into a single dataframe

  """
  if len(dfs):
    Spark_dfs = []
    for df in dfs:
      schema=creating_schema(df=df)
      # Convert Spark dataframe from Pandas 
      Spark_dfs.append(spark.createDataFrame(df,schema))
    # Union all spark dataframes including un-uniform columns
    Spark_df=unionAll(Spark_dfs)
  else:
     dbutils.notebook.exit("No Responding API Data"
  
 #Replace dot(.) with underscore ( _ ) from column names 
                           
  """
Removed dot(.) from columns name and replce with underscore ( _ )
"""
import re
tran_tab = str.maketrans({x:None for x in list('{()}')})
df= Spark_df.toDF(*(re.sub(r'[\.\s]+', '_', c).translate(tran_tab) for c in Spark_df.columns))
df=df.select(sorted(df.columns))
        
# Add Missing column in the dataframe                        
columns_list=["countryID, CountryName","Population","Language"]
for column_name in columns_list:
  df = df.withColumn(column_name, detect_column(column_name, df, StringType()))
print(len(df.columns))

# Write file spark df to pandas with filename datetime
                           
from datetime import date,datetime,timedelta
 
from datetime import datetime,date,timedelta
import pytz 
utc=datetime.now()
nz=utc.astimezone(pytz.timezone('Pacific/Auckland'))
filedate=nz.strftime("%Y-%m-%d-%H%M%S")
print(filedate)

# spark df to pandas df
try:
  pandasdf=df.toPandas()
                           
except Exception as e:
  print(e)

# write file in csv
 destination= f'/mnt/data/{pFileType}'
                           
ExportPath = f"{destination}/{pFileType}_{filedate}.csv"
print(f"Export Path Location: {ExportPath}")
print("Write CSV")
pandasdf.to_csv(path_or_buf= '/dbfs'+ ExportPath ,index=False)
                        


  
