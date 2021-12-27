# setup with python


Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
dbUsername = dbutils.secrets.get(scope = "Scope_Name", key = "Your_UserName_Key")      
dbPwd = dbutils.secrets.get(scope = "Scope_Name", key = "your_dbPwd_Key")           
Hostname = "Your_SQLSERVER_HOSTNAME" 
Port = "your_sql_server_port"
Database_Name = "Your_SQLSERVER_DB_NAME"                                                                    

##Connection Strings
Url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password= {4}".format(Hostname, Port, Database_Name, dbUsername, dbPwd)       

#https://medium.com/delaware-pro/executing-ddl-statements-stored-procedures-on-sql-server-using-pyspark-in-databricks-2b31d9276811
# Fetch the driver manager from your spark context
driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
# Create a connection object using a jdbc-url, + sql uname & pass
con = driver_manager.getConnection(Url, dbUsername, dbPwd)



# Setup with scala


%scala
val Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
val dbUsername = dbutils.secrets.get(scope = "Scope_Name", key = "Your_UserName_Key");
val dbPwd = d dbutils.secrets.get(scope = "Scope_Name", key = "your_dbPwd_Key");           
val Hostname = "Your_SQLSERVER_HOSTNAME" ;            
val Port = 1433;            
val Database_Name = "Your_SQLSERVER_DB_NAME";                                                                         
//Connection String
val Url = s"jdbc:sqlserver://${Hostname}:${Port};database=${Database_Name};user=${dbUsername};password=${dbPwd}";   


# Read from sql server
Table= "country"
Query="Select * from {table}"

  ##Read Query
df = spark.read.format("jdbc")\
    .option("driver", Driver)\
    .option("url", Url)\
    .option("query", Query) \
    .load()

  # Write
df.write.mode('overwrite').option("truncate",True).jdbc(Url, Table)

# Execute statements for delete existing data 
  statement = \
  """
  Delete
    dbs.country 
  where
    country_name = '{0}'
  """.format(country_name)
  
# Create callable statement and execute it
exec_statement = con.prepareCall(statement)
exec_statement.execute()

# appened new data into the sql table

 df_new_data.write.option("batchsize", 4000).jdbc(Url, Table, mode="append")

# Merge statement execution example:
statement = \
"""
merge country as c
using new_data n
on c.country_id=n.country_id
when not matched 
   then insert *
when matched 
  then update *
"""
exec_statement = con.prepareCall(statement)
exec_statement.execute()


# Close connections
exec_statement.close()
