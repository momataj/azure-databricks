
# Read Data from SQL Database Table using Pyspark

def sqlvm_jdbc_connection(DatabaseName):   
  """ 
  Store credentail Azure vault and get the credentials from ecret scope 
  """
  
  Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  dbUsername = dbutils.secrets.get(scope = "scope_name", key = "scope_key")      # user name from the secret scope
  dbPwd = dbutils.secrets.get(scope = "scope_name", key = "scope_key")           # password from the secret scope
  Hostname = dbutils.secrets.get(scope = "scope_name", key = "scope_key")            # SQL Server name
  Port = dbutils.secrets.get(scope = "scope_name", key = "scope_key")                # DB Port
    
  Database = DatabaseName # DB name
  Url = (f"jdbc:sqlserver://{Hostname}:{Port};database={Database};user={dbUsername};password= {dbPwd}")
  parameters = {
      "driver":Driver,
      "url":Url      
      }
  return parameters

def read_db_data(SQL_Query,DatabaseName):
  # Create function of read db table and create dataframe 
  Credentials=sqlvm_jdbc_connection(DatabaseName=DatabaseName)
  try: 
    # Read Query
    queryDF = spark.read.format("jdbc")\
        .option("driver", Credentials['driver'])\
        .option("url", Credentials['url'])\
        .option("fetchsize",50000)\
        .option("query", SQL_Query) \
        .load()
  except Exception as e:
    print(e.args)
  return queryDF


def sql_query(TableName):
  sql_query="""
      Select * from """+TableName+"""
      """
  return sql_query

def main():

  DatabaseName='ABC' #name of the database
  SQL_Query=sql_query(TableName)
  queryDF=read_db_data(SQL_Query,DatabaseName)

  # create a temp view in spark
  queryDF.createOrReplaceTempView("queryDF")
  print(queryDF.count())
  queryDF.display()

if __name__ == "__main__":
    main()
