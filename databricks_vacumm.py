# Create Parameters
# dbutils.widgets.text("DatabaseName", "")
# dbutils.widgets.text("RetainPeriod", "")
pDatabaseName = dbutils.widgets.get("DatabaseName")
pRetainPeriod = dbutils.widgets.get("RetainPeriod")
print(f'DatabaseName: {pDatabaseName}')
print(f'RetainPeriod: {pRetainPeriod}')

# Enable RetionDurationCheck
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

#Get Table Name from spark.catalog.listTables(database) except views

def GetTableName(DatabaseName):
  tables_collection = spark.catalog.listTables(DatabaseName)
  table_names_in_db=[]
  for table in tables_collection:
      if not table.name.startswith('vw_'):   # Avoid the view table 
        table_names_in_db.append(table.name )
  return table_names_in_db

#VacuumDryRun shows list of file will be vacummed

def VacuumDryRun(Database,Table,RetainPeriod):
  print("Vacumm Dry Run")
  VacuumDryRun=sql("""VACUUM """+Database+"""."""+Table+""" RETAIN """+RetainPeriod+""" HOURS DRY RUN """)
  VacuumDryRun.show(truncate = False)

#Vacumm will removed files from Delta’s transaction log + retention hours
def VacuumRun(Database,Table,RetainPeriod):
  print(f'VACUMM RUN: VACUUM {Database}.{Table} RETAIN {RetainPeriod} HOURS')
  VacuumDryRun=sql("""VACUUM """+Database+"""."""+Table+""" RETAIN """+RetainPeriod+""" HOURS """)
  VacuumDryRun.show(truncate = False)

#Main Program

def main():
    #Step1:RUN GetTableName
    tables_collection=GetTableName(DatabaseName=pDatabaseName)
    print(tables_collection)

    #Step2: RUN VacuumDryRun
    for TableName in tables_collection:
    print(f'TableName for Vacumm: {TableName}')
    VacuumDryRun(Database=pDatabaseName, Table=TableName,RetainPeriod=pRetainPeriod)
    print("DryRun Done; Check for Next Function")

    #VacuumRun function will be removed the list of files more than RetainPeriod
    #Step3: RUN VacuumRun
    for TableName in tables_collection:
    print(f'Vacumm TableName: {TableName}')  
    VacuumRun(Database=pDatabaseName, Table=TableName,RetainPeriod=pRetainPeriod)
    print("Vacumm Done")

if __name__ == "__main__":
    main()