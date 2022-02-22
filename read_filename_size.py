from pyspark.sql.functions import *
from datetime import *
import json
filepath='dbfs:/mnt/azureStorageAccountName/container_name/FolderName'
pFileList = dbutils.fs.ls(filepath) 
#convert list to a dataframe and exclude directories (size != 0)
fileRows = spark.createDataFrame(spark.sparkContext.parallelize(pFileList)).filter(col("size") != 0) 
 
display(fileRows)
