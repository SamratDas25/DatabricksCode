# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# COMMAND ----------

def processInParallel(filename):
    
    dbutils.widgets.text("storage_account_name","filename","sourcepath")
    dbutils.widgets.text("source_container","target_container","JobRunID")
    dbutils.widgets.text("JobID","JobName")
    dbutils.widgets.text("tablename","ErrorID","ObjectRunID")
    dbutils.widgets.text("adls2adls_scope","storage_account_key")
    dbutils.widgets.text("database_name","job_error_log_dbtable_name")
    dbutils.widgets.text("server_name","job_run_stats_dbtable_name")
    dbutils.widgets.text("ExecutionRunID","ObjectName")
    dbutils.widgets.text("log_directory","BalanceAmountColumn")
    

    server_name= dbutils.widgets.get("server_name")
    
   
    target_container= dbutils.widgets.get("target_container")
    JobName= dbutils.widgets.get("JobName")
    ObjectRunID= dbutils.widgets.get("ObjectRunID")
    storage_account_name= dbutils.widgets.get("storage_account_name")
    adls2adls_scope= dbutils.widgets.get("adls2adls_scope")
    ErrorID= dbutils.widgets.get("ErrorID")
    ExecutionRunID= dbutils.widgets.get("ExecutionRunID")
    ObjectName= dbutils.widgets.get("ObjectName")
    tablename= dbutils.widgets.get("tablename")
    job_run_stats_dbtable_name= dbutils.widgets.get("job_run_stats_dbtable_name")
    JobID= dbutils.widgets.get("JobID")
    database_name= dbutils.widgets.get("database_name")
    job_error_log_dbtable_name= dbutils.widgets.get("job_error_log_dbtable_name")
    JobRunID= dbutils.widgets.get("JobRunID")
    log_directory= dbutils.widgets.get("log_directory")
    storage_account_key= dbutils.widgets.get("storage_account_key")
    sourcepath= dbutils.widgets.get("sourcepath")
    source_container= dbutils.widgets.get("source_container") 
    BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")
    objectpath = dbutils.widgets.get("objectpath")

    dbutils.notebook.run(path = "./Ingestion-Pattern-ADLSRaw-to-ADLSCurated",
                                        timeout_seconds = 300, 
                                        arguments = {"filename":filename, "storage_account_name":storage_account_name, "sourcepath":sourcepath, "source_container":source_container, "target_container":target_container, "JobRunID":JobRunID, "JobID":JobID, "JobName":JobName, "tablename":tablename, "ErrorID":ErrorID, "ObjectRunID":ObjectRunID, "adls2adls_scope":adls2adls_scope, "storage_account_key":storage_account_key, "database_name":database_name, "job_error_log_dbtable_name":job_error_log_dbtable_name,  "server_name":server_name, "job_run_stats_dbtable_name":job_run_stats_dbtable_name, "ExecutionRunID":ExecutionRunID, "ObjectName":ObjectName, "log_directory":log_directory, "BalanceAmountColumn": BalanceAmountColumn, "objectpath": objectpath})


# COMMAND ----------

#FileNameList = ["employees.csv","employees1.csv","employees2.csv"]
dbutils.widgets.text("FileNameList","")
FileNames=dbutils.widgets.get("FileNameList")
print(FileNames)
FileNameList = FileNames.split(",")
print(FileNameList)
with ThreadPoolExecutor() as executor:
  results = executor.map(processInParallel,FileNameList)