#Install Custom Library in the Azure Databricks

## Install Packages in the workspace 

## Create a library folder and Install packages inside the library folder 
         -> Create Folder -> Library
         -> Library Folder-> (Rightclick)Create Library
                  * Select Library source : PYPI
                  * package: package name such as azure-storage-blob 
                  
         -> click on the package name and attach with the cluster. Each time cluster will provision, this package will install automatically in the cluster.
  

