# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# COMMAND ----------

def processInParallel(source_file_name):
    dbutils.widgets.text("account_name","share_name","directory_name")
    dbutils.widgets.text("source_file_name","scope")
    dbutils.widgets.text("keys","JobRunID","ExecutionRunID")
    dbutils.widgets.text("JobID","JobName")
    dbutils.widgets.text("database_name","sql_database")
    dbutils.widgets.text("job_run_stats_dbtable_name","error_run_stat_dbtable_name")
    dbutils.widgets.text("ObjectRunID","ErrorID")
    dbutils.widgets.text("ObjectName","ErrorCode","log_container_name")
    dbutils.widgets.text("storage_account_url","sqluserkey","sqlpasskey")
    dbutils.widgets.text("container_name","BalanceAmountColumn")
    
    
    account_name=dbutils.widgets.get("account_name")
    share_name=dbutils.widgets.get("share_name")
    directory_name=dbutils.widgets.get("directory_name")
    scope=dbutils.widgets.get("scope")
    keys=dbutils.widgets.get("keys")
    JobRunID=dbutils.widgets.get("JobRunID")
    ExecutionRunID=dbutils.widgets.get("ExecutionRunID")
    JobID=dbutils.widgets.get("JobID")
    JobName=dbutils.widgets.get("JobName")
    database_name=dbutils.widgets.get("database_name")
    sql_database=dbutils.widgets.get("sql_database")
    job_run_stats_dbtable_name=dbutils.widgets.get("job_run_stats_dbtable_name")
    error_run_stat_dbtable_name=dbutils.widgets.get("error_run_stat_dbtable_name")
    ErrorID=dbutils.widgets.get("ErrorID")
    ObjectRunID=dbutils.widgets.get("ObjectRunID")
    ObjectName=dbutils.widgets.get("ObjectName")
    ErrorCode=dbutils.widgets.get("ErrorCode")
    log_container_name=dbutils.widgets.get("log_container_name")
    storage_account_url=dbutils.widgets.get("storage_account_url")
    sqluserkey=dbutils.widgets.get("sqluserkey")
    sqlpasskey=dbutils.widgets.get("sqlpasskey")
    container_name=dbutils.widgets.get("container_name")
    BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")
    sourcepath = dbutils.widgets.get("sourcepath")
    
    
    
    dbutils.notebook.run(path = "./Ingestion-Pattern-NAS-to-ADLS-V2",
                                        timeout_seconds = 300, 
                                        arguments = {"source_file_name":source_file_name,"account_name":account_name,"share_name":share_name,"directory_name":directory_name,"scope":scope,"keys":keys,"JobRunID":JobRunID,"ExecutionRunID":ExecutionRunID,"JobID":JobID,"JobName":JobName,"database_name":database_name,"sql_database":sql_database,"job_run_stats_dbtable_name":job_run_stats_dbtable_name,"error_run_stat_dbtable_name":error_run_stat_dbtable_name,"ErrorID":ErrorID,"ObjectRunID":ObjectRunID,"ObjectName":ObjectName,"container_name":container_name,"ErrorCode":ErrorCode,"log_container_name":log_container_name,"storage_account_url":storage_account_url,"sqluserkey":sqluserkey,"sqlpasskey":sqlpasskey, "BalanceAmountColumn": BalanceAmountColumn,"sourcepath": sourcepath})


# COMMAND ----------


#FileNameList1 = ["employees.csv","employees1.csv","employees2.csv"]
dbutils.widgets.text("FileNameList","")
FileNames=dbutils.widgets.get("FileNameList")
print(FileNames)
FileNameList = FileNames.split(",")
print(FileNameList)
with ThreadPoolExecutor() as executor:
  results = executor.map(processInParallel,FileNameList)