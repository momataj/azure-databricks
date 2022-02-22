StorageMountKey = dbutils.secrets.get(scope="scope_Name",key="secret-key")
straccount = "MountStorageName"

#Mount Market Monitoring Blob Storage
configs = {"fs.azure.account.key.MountStorageName.blob.core.windows.net":StorageMountKey}

dbutils.fs.mount(
  source = "wasbs://Container_name@MountStorageName.blob.core.windows.net",
  mount_point = "/mnt/MountStorageName",
  extra_configs = configs
)

# Optionally, you can add <directory-name> to the source URI of your mount point.

dbutils.fs.mount(
  source = "wasbs://Container_name@MountStorageName.blob.core.windows.net",
  mount_point = "/mnt/Container_name",
  extra_configs = configs
)
