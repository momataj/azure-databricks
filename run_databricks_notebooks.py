import uuid
BatchGuid = str(uuid.uuid4()) 

ExceptionsArray = []  # declare an excetionArrary to check error in case notebooks failure. 

#%run ./hello_world   Test notebook run with this command 

try:
  dbutils.notebook.run("./hello_world", 600, {"parameter": "country", "batch_id": BatchGuid})  # Parameters from notebook 
  dbutils.notebook.run("./word_count", 600, {})  # Without parameters 
  

except Exception as e:
  print(e)
  ExceptionsArray.append(e)


if len(ExceptionsArray)  > 0:
  raise ExceptionsArray[0]
  
dbutils.notebook.exit('Complete')  
