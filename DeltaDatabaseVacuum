# ClassMethod: Get Table Name from spark.catalog.listTables(database) except views and others temp tables and Run Vacuum
class DeltaDatabaseVacuum:
  def __init__(self,DatabaseName, TableInitial):
    self.DatabaseName=DatabaseName
    self.TableInitial=TableInitial
  
  def _getTableName(self):  #get Name Instance function
    table_collection = spark.catalog.listTables(self.DatabaseName)
    tables_collectionlist=table_collection
    table_names_in_db=[]
    if self.TableInitial=='None':
      for table in tables_collectionlist:
        # Avoid the view table 
        if not table.name.startswith('vw') and not table.name.endswith('vw') and not table.name.startswith('rpt') and not table.name.endswith('rpt'):  
          table_names_in_db.append(table.name)
    else:
      for table in tables_collectionlist:
        # Avoid the view table
        if not table.name.startswith('vw') and  not table.name.endswith('vw') and not table.name.startswith('rpt') and not table.name.endswith('rpt') and  table.name.startswith(self.TableInitial):
          table_names_in_db.append(table.name )      
    return table_names_in_db    
  
  @classmethod      #vacumm classmethod function
  def _runVacuum(cls,DatabaseName,TableName,RetainPeriod):
    cls.DatabaseName=DatabaseName
    cls.TableName=TableName
    cls.RetainPeriod=RetainPeriod
    print(f'VACUMM RUN: VACUUM {cls.DatabaseName}.{cls.TableName} RETAIN {cls.RetainPeriod} HOURS')
    VacuumRun=sql("""VACUUM """+cls.DatabaseName+"""."""+cls.TableName+""" RETAIN """+cls.RetainPeriod+""" HOURS """)
    VacuumRun.show(truncate = False)

# Execute Function
DatabaseName=DeltaDatabaseVacuum(DatabaseName=pDatabaseName,TableInitial=pTableInitial )
tables_collection=DeltaDatabaseVacuum._getTableName(DatabaseName)
print(f'Total tables starting with {pTableInitial.capitalize()} for Database {pDatabaseName.capitalize()}: {len(tables_collection)}')

for TableName in tables_collection:
  print(f'Vacumm TableName: {TableName}')  
  DeltaDatabaseVacuum._runVacuum(DatabaseName=pDatabaseName,TableName=TableName,RetainPeriod=pRetainPeriod)
print("Vacumm Done")
