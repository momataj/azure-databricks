"""
Run Multiple databricks notebooks at a time from one Notebook using python

Example: Run end to end solution for data pipleines 
- Data ingestion application Notebook
- Transformation application Notebook
- Refine application Notebook

Call all above applications in run_databricks_notebooks and schedule them using apache airflow/Azure data factory
"""

import uuid
BatchGuid = str(uuid.uuid4()) 

ExceptionsArray = []  # declare an excetionArrary to check error in case notebooks failure. 

#%run ./hello_world   Test notebook run with this command 

try:
  dbutils.notebook.run("./Data_ingestion", 600, {"parameter": "country", "batch_id": BatchGuid})  # Parameters from notebook 
  dbutils.notebook.run("./Transformation", 600, {"parameter": "country", "batch_id": BatchGuid}) 
  dbutils.notebook.run("./Refine", 600, {})  # Without parameters  
  

except Exception as e:
  print(e)
  ExceptionsArray.append(e)


if len(ExceptionsArray)  > 0:
  raise ExceptionsArray[0]
  
dbutils.notebook.exit('Complete')  
