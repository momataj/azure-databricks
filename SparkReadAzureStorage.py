def Storage_Connection(AzureStorageName,ContainerName):
  
  #Initialise File System for the Notebook - Initialising the file system much like you would for a network drive
  spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true") #Initialises
  spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false") #Prvents it from reinitialising constantly

  # access storage account using access key
  spark.conf.set(
    "fs.azure.account.key.{AzureStorageName}.blob.core.windows.net",
    dbutils.secrets.get(scope="scope_name",key="secret-storage-key"))

  storageAccount = (f"wasbs://{ContainerName}@{AzureStorageName}.blob.core.windows.net")
  return storageAccount

def ReadSpark(FolderPath):
  # Read Spark
  df=spark.read.csv(FolderPath, header=True)

  # create a temp view in spark
  df.createOrReplaceTempView("df") 
  
  return df


def main():

  AzureStorageName='ABC' #name of the Storage
  ContainerName='data'  #name of container
  storageAccount=Storage_Connection(AzureStorageName,ContainerName)
  FolderPath=f'{storageAccount}/fileName.csv'
  df=ReadSpark(FolderPath)
  df.display()
  print(df.count())



if __name__ == "__main__":
    main()