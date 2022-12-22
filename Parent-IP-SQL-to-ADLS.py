# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# COMMAND ----------

def processInParallel(TableName):
    
    dbutils.widgets.text("storage_account_name","SourceName","TargetName")
    dbutils.widgets.text("JobRunID","JobName","JobId")
    dbutils.widgets.text("TargetPath","ExecutionRunID","server_name")
    dbutils.widgets.text("database_name","server_name","sourcetable_name")
    dbutils.widgets.text("full_load","push_query","job_run_stats_dbtable_name")
    dbutils.widgets.text("job_error_log_dbtable_name","ObjectRunID","ObjectName")
    dbutils.widgets.text("target_container","ErrorID","BatchRunID")
    dbutils.widgets.text("log_directory","BalanceAmountColumn","objectpath")
    dbutils.widgets.text("sourcepath","filename","folder_name")
    dbutils.widgets.text("db_scope","")

    

    

    server_name= dbutils.widgets.get("server_name")
    Targetpath= dbutils.widgets.get("Targetpath")
    TargetName= dbutils.widgets.get("TargetName")
    JobName= dbutils.widgets.get("JobName")
    storage_account_name= dbutils.widgets.get("storage_account_name")
    SourceName= dbutils.widgets.get("SourceName")
    ErrorID= dbutils.widgets.get("ErrorID")
    ExecutionRunID= dbutils.widgets.get("ExecutionRunID")
    ObjectName= dbutils.widgets.get("ObjectName")
    job_run_stats_dbtable_name= dbutils.widgets.get("job_run_stats_dbtable_name")
    JobID= dbutils.widgets.get("JobID")
    database_name= dbutils.widgets.get("database_name")
    job_error_log_dbtable_name= dbutils.widgets.get("job_error_log_dbtable_name")
    JobRunID= dbutils.widgets.get("JobRunID")
    sourcepath= dbutils.widgets.get("sourcepath")
    sourcetable_name = dbutils.widgets.get("sourcetable_name")
    push_query = dbutils.widgets.get("push_query")
   # max_date = dbutils.widgets.get("max_date")
    full_load = dbutils.widgets.get("full_load")    
    ObjectRunID= dbutils.widgets.get("ObjectRunID")
    target_container = dbutils.widgets.get("target_container")
    BatchRunID = dbutils.widgets.get("BatchRunID")
    log_directory = dbutils.widgets.get("log_directory")
    BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")
    filename = dbutils.widgets.get("filename")
    objectpath = dbutils.widgets.get("objectpath")
    folder_name = dbutils.widgets.get("folder_name")
    db_scope = dbutils.widgets.get("db_scope")

    dbutils.notebook.run(path = "./Ingestion-Pattern-SQL-to-ADLS",
                                        timeout_seconds = 300, 
                                        arguments = {"TableName": TableName, "storage_account_name":storage_account_name,"target_container":target_container, "JobRunID":JobRunID, "JobID":JobID, "JobName":JobName, "SourceName":SourceName, "ErrorID":ErrorID, "ObjectRunID":ObjectRunID, "target_container":target_container, "database_name":database_name, "job_error_log_dbtable_name":job_error_log_dbtable_name,  "server_name":server_name, "job_run_stats_dbtable_name":job_run_stats_dbtable_name, "Targetpath":Targetpath, "ExecutionRunID":ExecutionRunID, "TargetName":TargetName, "BatchRunID":BatchRunID, "ObjectName":ObjectName,"ObjectRunID":ObjectRunID, "push_query": push_query, "log_directory": log_directory, "sourcetable_name": sourcetable_name, "folder_name": folder_name, "BalanceAmountColumn": BalanceAmountColumn, "objectpath": objectpath, "sourcepath": sourcepath, "filename": filename, "db_scope": db_scope, "full_load": full_load})


# COMMAND ----------

#FileNameList = ["employees.csv","employees1.csv","employees2.csv"]

dbutils.widgets.text("TableNameList","")
TableName=dbutils.widgets.get("TableNameList")
print(TableName)
TableNameList = TableName.split(",")
print(TableNameList)
with ThreadPoolExecutor() as executor:
  results = executor.map(processInParallel,TableNameList)