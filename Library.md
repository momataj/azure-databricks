# Install Custom Library in the Azure Databricks

## 1.Install Packages on a cluster which is alreary uploaded to the workspace
## Workspace library
### Create a library folder and Install packages inside the library folder 

    -> Create Folder -> Library
        -> Library Folder-> (Rightclick)Create Library
             * Select Library source : PYPI
             * package: package name such as azure-storage-blob 
                  Or for install specific version:
                    <Library>==<Version>    example: azure-storage-blob==12.9.0
                  
         a. click on the library  package name 
         b. Select **Install automatically on all cluster** checkbox
         c. Confirm
         /* Each time cluster will provision, this package will install automatically in the cluster. */
## Cluster-installed library     
    a. click on **Computer** in the sidebar
    b. Click Cluster Name
    c. Click  Libraries tab
    d. Click Install New.
    e. Follow one of the method which suited you and click on library source type and name of the package/version. Then click on Create, the library will be installed on the cluster.


## 2. Install packages in the notebook
 
  ### Option1: 
    Install packages in the notebook using the magic command
            %pip install azure-storage-blob 
  ### Option2: 
    Create a notebook and note down all required Python libraries in a notebook in following way:
        dbutils.library.installPyPI("azure")
        dbutils.library.installPyPI("scikit-learn", version="1.19.1")
        dbutils.library.restartPython()  
    then call the notebook using the magic command:
 
          %run pathofthe notebook(notebook_install_lib)   

  ### Option3: 
    Create a requirements.txt file in the dbfs and install the packages in the notebook.  This is easy maintenance for all common python packages we need to install 

            %pip install -r /dbfs/requirements.txt

## Install R Packages in the Notebook
    **Install R packages in the Python notebook**
        %r
        install.packages("R.utils")
    **Install R packages in the R notebook**
         install.packages("R.utils")
     
     
## 3. Install packages in the cluster using init script
        #!/bin/bash
        /databricks/python/bin/pip install azure-storage-blob
        


            


         


