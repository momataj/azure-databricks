from pyspark.sql.functions import *
from datetime import *
import json


InputSourceLocation = "/mnt/azureStorageName/Container_name"
filepath = 'dbfs:'+ InputSourceLocation + 'folder/'
print(filepath)

df = (spark \
               .read \
               .csv(filepath) \
               .withColumn("FileName",regexp_replace(input_file_name(), "%20", " "))\
                              
              )

df.display()
