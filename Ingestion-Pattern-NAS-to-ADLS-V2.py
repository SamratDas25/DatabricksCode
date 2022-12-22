# Databricks notebook source
pip install azure azure-storage azure-servicebus azure-mgmt azure-servicemanagement-legacy

# COMMAND ----------

# Reading from Parameter

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
# Reading from fileshare and writing to Adls containers

############Importing packages######

from datetime import datetime, timedelta
from azure.storage.file import FileService
from azure.storage.file import FilePermissions
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import *
import sys
import os

#/Workspace/Repos/DAP-Repo/Databricks/main.py
 
# In the command below, replace <username> with your Databricks user name.
sys.path.append(os.path.abspath('/Workspace/Repos/DAP-Repo/Databricks'))
sys.path.append(os.path.abspath('/Workspace/Repos/DAP-Repo/Databricks/ABCUtilities'))


# from validations import wrapperFunction
from validations import wrapperFunction
from errorlog import Error_log


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

logs_info = []
time  = datetime.now()
current_time  = datetime.now()
start_time  = datetime.now()
def secrets(scopes,keys):
    secret_scope = dbutils.secrets.get(scopes,keys)
    return secret_scope

log = f"{time} info {NOTEBOOK_PATH} Created notebook_path variable"
logs_info.append(log)
# created notebook_path variable
log = f"{time} info {NOTEBOOK_PATH} created time variable for diplaying current time"
logs_info.append(log)
# created time variable for diplaying current time
sourceCount=0
TargetCount=0

#Declared global variables for future use
    
##### validation checks #####

log = f"{time} info {NOTEBOOK_PATH} Generating Job Rule Validation"
logs_info.append(log)
#### Generating Job Rule Validation

sqluserkey= dbutils.widgets.get("sqluserkey")
log = f"{time} info {NOTEBOOK_PATH} Accessing sqluserkey "  
logs_info.append(log)
# Accessing sqluserkey

sqlpasskey= dbutils.widgets.get("sqlpasskey")
log = f"{time} info {NOTEBOOK_PATH} Accessing sqlpasskey "  
logs_info.append(log)
# Accessing sqluserkey

scope_value = dbutils.widgets.get("scope")
log = f"{time} info {NOTEBOOK_PATH} Accessing scope_value {scope_value}"  
logs_info.append(log)
# Accessing scope_value
    
keys_value = dbutils.widgets.get("keys")
log = f"{time} info {NOTEBOOK_PATH} Accessing keys_value {keys_value}"  
logs_info.append(log)
# Assigned keys_value 
storage_account_url= dbutils.widgets.get("storage_account_url")
log = f"{time} info {NOTEBOOK_PATH} Accessing storage_account_url {storage_account_url}"  
logs_info.append(log)
# Accessing storage_account_url
spark.conf.set(storage_account_url,secrets(scope_value, keys_value))
log = f"{time} info {NOTEBOOK_PATH} Configuring storage account"
logs_info.append(log)
# Configuring storage account
account_key = secrets(scope_value, keys_value)
log = f"{time} info {NOTEBOOK_PATH} Retreive scope & keys from databricks keyvault account_key"  
logs_info.append(log)
# Retreive account_key from secrets scopes and keys
jdbcUsername = secrets(scope_value, sqluserkey)
log = f"{time} info {NOTEBOOK_PATH} Retreive scope & keys from databricks keyvault jdbcUsername"  
logs_info.append(log)
# Retreive jdbcUsername from secrets scopes and keys
jdbcPassword = secrets(scope_value, sqlpasskey)
log = f"{time} info {NOTEBOOK_PATH} Retreive scope & keys from databricks keyvault jdbcPassword "  
logs_info.append(log)
##### validation checks #####  

log = f"{current_time} info {NOTEBOOK_PATH} Creating a variables for Job Rule Validation"
logs_info.append(log)
# Creating a variables for Job Rule Validation
source_file_name = dbutils.widgets.get("source_file_name")
log = f"{time} info {NOTEBOOK_PATH} Accessing source_file_name {source_file_name}"  
logs_info.append(log)
# Accessing source_file_name

source_file_name_value = source_file_name
filename = source_file_name
sourcepath = dbutils.widgets.get("sourcepath")
BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")

server_name = dbutils.widgets.get("sql_database")
database_name = dbutils.widgets.get("database_name")
JobName = dbutils.widgets.get("JobName")


filenamelist=filename.split('_')

filenamelist1=filenamelist[1].split('.')

filename = filenamelist[0] + '.' + filenamelist1[1]
  
datetime_str = filenamelist1[0]
datetime_list=datetime_str.split('T')
date_obj = datetime.strptime(datetime_list[0], "%Y%m%d").date()
time_obj = datetime.strptime(datetime_list[1], "%H%M%S").time()
 
hours = time_obj.strftime("%H")
date = date_obj.strftime("%d")
month = date_obj.strftime("%m")
year = date_obj.strftime("%Y")
 
given_source_name = filenamelist[0]
given_format = filenamelist1[1]
 
logfilename = filenamelist[0] +"_"+ filenamelist1[0]
### end filename extraction ###

account_name_value = dbutils.widgets.get("account_name")
share_name_value = dbutils.widgets.get("share_name")
directory_value = dbutils.widgets.get("directory_name")

source_path = f"https://{account_name_value}.file.core.windows.net/{share_name_value}/{directory_value}/{source_file_name_value}"

querydata = "select JobId from metaabc.Job_Metadata where JobName ="+"'"+JobName+"'"

JobId_df = (spark.read
  .format("jdbc")\
  .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}")\
  .option("query", querydata)\
  .option("user", jdbcUsername)\
  .option("password", jdbcPassword)\
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
  .load()
)

JobId=JobId_df.collect()[0][0]

log = f"{current_time} info {NOTEBOOK_PATH} Calling wrapper function by passing Sourcepath, filename, BalanceAmountColumn"
logs_info.append(log)
# Calling wrapper function by passing Sourcepath, filename, BalanceAmountColumn
ObjectType = 'File'
validationResults = wrapperFunction(JobId, sourcepath, filename, BalanceAmountColumn,"File","",server_name,database_name)

log = f"{current_time} info {NOTEBOOK_PATH} Generated Job Rule Validation"
logs_info.append(log)
# Generated Job Rule Validation 

log = f"{current_time} info {NOTEBOOK_PATH} Validation Results : {validationResults}"
logs_info.append(log)
# Log with Rule Validation Results 



###### Accessing  parameter Values  #############

log = f"{time} info {NOTEBOOK_PATH} Extracting year month date hours from the current time"  
logs_info.append(log)
# Assigned keys_value 


# Reading parameters from Widgests
try:
        
    account_name_value = dbutils.widgets.get("account_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing account_name_value {account_name_value}"  
    logs_info.append(log)
    # Accessing account_name_value
    
    share_name_value = dbutils.widgets.get("share_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing share_name_value {share_name_value}"  
    logs_info.append(log)
    # Accessing share_name_value
    
    directory_value = dbutils.widgets.get("directory_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing directory_value {directory_value}"  
    logs_info.append(log)
    # Accessing directory_value
    
    JobRunID = dbutils.widgets.get("JobRunID")
    JobRunID = int(JobRunID)
    log = f"{time} info {NOTEBOOK_PATH} Accessing JobRunID {JobRunID}"  
    logs_info.append(log)
    # Accessing JobRunID 
    
    ExecutionRunID = dbutils.widgets.get("ExecutionRunID")
    ExecutionRunID = int(ExecutionRunID)
    log = f"{time} info {NOTEBOOK_PATH} Accessing ExecutionRunID {ExecutionRunID}"  
    logs_info.append(log)
    # Accessing ExecutionRunID
    
    JobID = dbutils.widgets.get("JobID")
    JobID = JobId
    log = f"{time} info {NOTEBOOK_PATH} Accessing JobID {JobID}"  
    logs_info.append(log)
    # Accessing JobID
    
    JobName = dbutils.widgets.get("JobName")
    log = f"{time} info {NOTEBOOK_PATH} Accessing JobName {JobName}"  
    logs_info.append(log)
    #  Accessing JobName
    
    database_name = dbutils.widgets.get("database_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing database_name {database_name}"  
    logs_info.append(log)
    # Accessing database_name
    
    job_run_stats_table_name = dbutils.widgets.get("job_run_stats_dbtable_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing job run stats dbtable_name {job_run_stats_table_name}"  
    logs_info.append(log)
    # Accessing job run stats dbtable_name
     
    erro_run_stat_table_name = dbutils.widgets.get("error_run_stat_dbtable_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing job error log dbtable_name {erro_run_stat_table_name}"  
    logs_info.append(log)
    # Accessing job error log dbtable_name
    
    ErrorID = dbutils.widgets.get("ErrorID")
    log = f"{time} info {NOTEBOOK_PATH} Accessing ErrorID {ErrorID}"  
    logs_info.append(log)
    # Accessing ErrorID
    
    ObjectRunID = dbutils.widgets.get("ObjectRunID")
    log = f"{time} info {NOTEBOOK_PATH} Accessing ObjectRunID {ObjectRunID}"  
    logs_info.append(log)
    # Accessing ObjectRunID

    ObjectName = dbutils.widgets.get("ObjectName")
    log = f"{time} info {NOTEBOOK_PATH} Accessing ObjectName {ObjectName}"  
    logs_info.append(log)
    # Accessing ObjectName
    
    ErrorCode = dbutils.widgets.get("ErrorCode")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ErrorCode {ErrorCode}"  
    logs_info.append(log)
    # Assigned ErrorCode
   
    log_container_name = dbutils.widgets.get("log_container_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing log_path {log_container_name}"  
    logs_info.append(log)
    # Accessing log_path
    
    server_name = dbutils.widgets.get("sql_database")
    log = f"{time} info {NOTEBOOK_PATH} Accessing sql_database {server_name}"
    logs_info.append(log)
    # Accessed sql_database
    
    container_name = dbutils.widgets.get("container_name")
    log = f"{time} info {NOTEBOOK_PATH} Accessing container_name "
    logs_info.append(log)
    # Accessing container_name
    
    current_time = datetime.now()
    targetpath_value = f"abfss://{container_name}@{account_name_value}.dfs.core.windows.net/nasdrivefiles/prod/raw/healthdata/" 
    log = f"{time} info {NOTEBOOK_PATH} Accessing targetpath_value {targetpath_value}"
    logs_info.append(log)
    # Accessing targetpath_value
     
    log_path = f"abfss://{log_container_name}@{account_name_value}.dfs.core.windows.net/"
    log = f"{time} info {NOTEBOOK_PATH} Accessing log_path {log_path}"
    logs_info.append(log)
    # Accessing log_path
    #log_date = time.strftime("%Y-%m-%d")
    log_date = f"{year}/{month}/{date}/{hours}"
    log = f"{time} info {NOTEBOOK_PATH} Generating Log date path {log_date}"
    logs_info.append(log)
    # Accessing Generating Log date path
    
    now=datetime.now()
    d = now.strftime("%Y%m%d")
    t = now.strftime("%H%M%S")
    currenttimestamp=str(d) +"T"+ str(t)
    log_path_latest = f"{log_path}/Log_directory/{log_date}/" + "log_NASDRIVE_to_RAWLayer_" + logfilename + "_" + currenttimestamp +" log.txt" 
    log = f"{time} info {NOTEBOOK_PATH} Final Log Path {log_path_latest}"
    logs_info.append(log)
    # Accessing log_path_latest
    
except Exception as ex: 
    current_time  = datetime.now()
    error_message = ex
    log = f"{time} info {NOTEBOOK_PATH} error_message {error_message}"
    logs_info.append(log)
    # accessing error type 
    error_type = type(ex).__name__
    log = f"{time} info {NOTEBOOK_PATH} error type {error_type}"
    logs_info.append(log)
    # accessing error type
    log = f"{time} info {NOTEBOOK_PATH} Error Occured while reading and assigning parameters {error_type}, {error_message}"
    logs_info.append(log)
    
    Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, erro_run_stat_table_name = erro_run_stat_table_name, jdbcUsername = jdbcUsername, jdbcPassword = jdbcPassword)
    
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error occurred while configuring Parameters")



############# Generating Job_Run_Stats #################
time  = datetime.now()
log = f"{time} info {NOTEBOOK_PATH} Generating Initial Job_Run_Stats"
logs_info.append(log)
log = f"{time} info {NOTEBOOK_PATH} Creating Job_Run_Stats schema for the Dataframe"
logs_info.append(log)


try:
    Job_Run_Stats = [{"JobRunID": JobRunID,
                      "ExecutionRunID": f"{ExecutionRunID}",
                      "JobID": JobID,
                      "JobName": f"{JobName}",
                      "SourceSystem": f"NAS DRIVE", 
                      "SourcePath": f"{source_path}",
                      "TargetSystem": f"ADLS", 
                      "TargetPath": f"rawlayer",
                      "SourceCount": 0,
                      "TargetCount":  0, 
                      "RejectCount": 0, 
                      "JobStartTime": start_time, 
                      "JobEndTime": '0', 
                      "DurationInSeconds": 0, 
                      "Status": 'Started'}]
    
    log = f"{time} info {NOTEBOOK_PATH} Job_Run_Stats schema has been created"
    logs_info.append(log)
    # Job_Run_Stats schema has been created
    log = f"{time} info {NOTEBOOK_PATH} Dataframe creation and Datatype casting started"
    logs_info.append(log)
    # Dataframe creation and Datatype casting started
    jobdf2 = spark.createDataFrame(Job_Run_Stats)
    jobdf2 = jobdf2.withColumn("DurationInSeconds",jobdf2.DurationInSeconds.cast("int"))
    jobdf2 = jobdf2.withColumn("JobStartTime",to_timestamp("JobStartTime"))
    jobdf2 = jobdf2.withColumn("JobEndTime",to_timestamp("JobEndTime"))
    
    log = f"{time} info {NOTEBOOK_PATH} Dataframe created and Datatype casting completed"
    logs_info.append(log)
    # Dataframe created and Datatype casting completed
    log = f"{time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    # Creating Connection and started writing to SQL Database
    jobdf2.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_run_stats_table_name}")\
              .option("user", f"{jdbcUsername}")\
              .option("password", f"{jdbcPassword}")\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    #Created Connection and completed writing to SQL Database
    log = f"{time} info {NOTEBOOK_PATH} Generated Initial Job_Run_Stats"
    logs_info.append(log)
    # Generated Initial Job_Run_Stats
except Exception as ex:
    time  = datetime.now()
    error_message = ex
    log = f"{time} info {NOTEBOOK_PATH} Error_message {error_message}"
    logs_info.append(log)
    # accessing error type 
    error_type = type(ex).__name__
    log = f"{time} info {NOTEBOOK_PATH} Error type {error_type}"
    logs_info.append(log)
    # accessing error type
    log = f"{time} info {NOTEBOOK_PATH}  Error occurred while Generating Initial Job Run stats {error_type}"
    logs_info.append(log)
    # Error Occured while reading and assigning parameters
    #print(f"Error occurred while Generating Initial Job Run stats {error_type}, {error_message}")
    #Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID)
    Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, erro_run_stat_table_name = erro_run_stat_table_name, jdbcUsername = jdbcUsername, jdbcPassword = jdbcPassword)
    
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error occurred while Generating Job_Run_Stats")
    

# reading file from fileshare write into adls containers
 
try:
    time  = datetime.now()
    file_service = FileService(account_name=account_name_value, account_key=account_key)
    log = f"{time} info {NOTEBOOK_PATH} Created File_service {file_service} "
    logs_info.append(log)
    # Assigned File_service
    #print(file_service)
    
    sas_token = file_service.generate_file_shared_access_signature(share_name_value, directory_value, source_file_name_value, permission=FilePermissions.READ,     expiry=datetime.utcnow() + timedelta(hours=1))

    log = f"{time} info {NOTEBOOK_PATH} Generated sas_token value {sas_token} "
    logs_info.append(log)
    # generating sas_token value
    #print(sas_token)
    source_path = f"https://{account_name_value}.file.core.windows.net/{share_name_value}/{directory_value}/{source_file_name_value}"
    log = f"{time} info {NOTEBOOK_PATH} Accessing source_path path {source_path}"
    logs_info.append(log)

    url_sas_token = f"https://{account_name_value}.file.core.windows.net/{share_name_value}/{directory_value}/{source_file_name_value}?{sas_token}"

    log = f"{time} info {NOTEBOOK_PATH} Generated url_sas_token value {url_sas_token} "
    logs_info.append(log)
    # generating url_sas_token value
    #print(url_sas_token)

    if given_format == "json":
        file = pd.read_json(url_sas_token, lines=True)
        log = f"{time} info {NOTEBOOK_PATH} Reading source file in json format  {url_sas_token} "
        logs_info.append(log)
        # reading source file in source format
        df = spark.createDataFrame(file)
        log = f"{time} info {NOTEBOOK_PATH} Creating into dataframe "
        logs_info.append(log)
        #  creating  into dataframe
        sourceCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating sourceCount value  {sourceCount} "
        logs_info.append(log)
        #print(sourceCount)
        # calculating sourceCount value
        structred_targetpath = f"{targetpath_value}/{given_source_name}/{year}/{month}/{date}/{hours}"
        df.coalesce(1).write.format("json").mode("append").option("header",True).save(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Writing dataframe in target path in Json format {structred_targetpath}"
        logs_info.append(log)
        # Writing dataframe in target path in Json format
        TargetCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating taragetCount value  {TargetCount} "
        logs_info.append(log)
        # Calculating taragetCount value
        list_f = dbutils.fs.ls(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Assigning  list of structred_targetpath to the variable"
        logs_info.append(log)
        # Assigning  list of structred_targetpath to the variable
        list_of_file_name_starts_with_part_ends_with_json = []
        log = f"{time} info {NOTEBOOK_PATH} creating a empty list for storing the part file that  prefix with  part and suffix with json format"
        logs_info.append(log) 
        # creating a empty list for storing the part file that  prefix with  part and suffix with format name
        for i in list_f:
            filenames = i.name
            if filenames.startswith("part") and filenames.endswith("json"):
                    list_of_file_name_starts_with_part_ends_with_json.append(filenames)
                    log = f"{time} info {NOTEBOOK_PATH} Accessing prefix part file names and ends with json format and stored in list "
                    logs_info.append(log) 
                    #appending file name  prefix with part file name and ends with format in list_of_file_name_starts_with_part
        length_of_part_file = len(list_of_file_name_starts_with_part_ends_with_json)
        for j in range(length_of_part_file):
            old_file_path = structred_targetpath + "/" + list_of_file_name_starts_with_part_ends_with_json[j]
            modified_name  = source_file_name_value
            new_file_path = structred_targetpath + "/" + modified_name
            #print(new_file_path, old_file_path)
            dbutils.fs.mv(old_file_path, new_file_path)
    elif given_format == "csv":
        file = pd.read_csv(url_sas_token)
        log = f"{time} info {NOTEBOOK_PATH} Reading source file in csv format {url_sas_token} "
        logs_info.append(log)
        # reading source file in source format
        df = spark.createDataFrame(file)
        log = f"{time} info {NOTEBOOK_PATH} Creating into dataframe"
        logs_info.append(log)
        #  creating  into dataframe
        sourceCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating sourceCount value {sourceCount} "
        logs_info.append(log)
        # calculating sourceCount value
        print(source_file_name_value)
        structred_targetpath = f"{targetpath_value}{given_source_name}/{year}/{month}/{date}/{hours}"
        print(structred_targetpath)
        #df.write.format("csv").mode("append").option("header",True).option(f"{structred_targetpath}",f"{given_source_name}")
        df.coalesce(1).write.format("csv").mode("append").option("header",True).save(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Writing dataframe in target path in csv format {structred_targetpath}"
        logs_info.append(log)
        # Writing dataframe in target path in CSV format
        TargetCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating taragetCount value {TargetCount} "
        logs_info.append(log)
        # calculating taragetCount value
        list_f = dbutils.fs.ls(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Assigning  list of structred_targetpath to the variable"
        logs_info.append(log)
        # Assigning  list of structred_targetpath to the variable
        list_of_file_name_starts_with_part_ends_with_csv = []
        log = f"{time} info {NOTEBOOK_PATH} creating a empty list for storing the part file that  prefix with  part and suffix with csv format"
        logs_info.append(log)
        # creating a empty list for storing the part file that  prefix with  part and suffix with format name
        print(source_file_name_value)
        for i in list_f:
            filenames = i.name
            if filenames.startswith("part") and filenames.endswith("csv"):
                    list_of_file_name_starts_with_part_ends_with_csv.append([filenames,source_file_name_value])
                    log = f"{time} info {NOTEBOOK_PATH} Accessing prefix part file names and ends with csv format and stored in list "
                    logs_info.append(log)
                    #appending file name  prefix with part file name and ends with format in list_of_file_name_starts_with_part
        length_of_part_file = len(list_of_file_name_starts_with_part_ends_with_csv)
        print(list_of_file_name_starts_with_part_ends_with_csv, length_of_part_file)
        for j in range(length_of_part_file):
            old_file_path = structred_targetpath + "/" + list_of_file_name_starts_with_part_ends_with_csv[j][0]
            new_file_path = structred_targetpath + "/" + list_of_file_name_starts_with_part_ends_with_csv[j][1] 
            print(new_file_path, old_file_path)
            dbutils.fs.mv(old_file_path, new_file_path)
    
    elif given_format == "xml":
        file = pd.read_xml(url_sas_token)
        log = f"{time} info {NOTEBOOK_PATH} Reading source file in xml format {url_sas_token} "
        logs_info.append(log)
        # reading source file in source format
        df = spark.createDataFrame(file)
        log = f"{time} info {NOTEBOOK_PATH} Creating into dataframe "
        logs_info.append(log)
        #  creating  into dataframe
        sourceCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating sourceCount value {sourceCount} "
        logs_info.append(log)
        # calculating sourceCount value
        structred_targetpath = f"{targetpath_value}/{given_source_name}/{year}/{month}/{date}/{hours}"
#         structred_targetpath = f"{targetpath_value}/{given_source_name}"
        df.coalesce(1).write.format("xml").mode("append").option("header",True).save(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Writing dataframe in target path in xml format {structred_targetpath}"
        logs_info.append(log)
        # Writing dataframe in target path in parquet format
        TargetCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating taragetCount value {TargetCount} "
        logs_info.append(log)
        # calculating taragetCount value  
        list_f = dbutils.fs.ls(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Assigning  list of structred_targetpath to the variable"
        logs_info.append(log)
        # Assigning  list of structred_targetpath to the variable
        list_of_file_name_starts_with_part_ends_with_xml = []
        
        log = f"{time} info {NOTEBOOK_PATH} creating a empty list for storing the part file that  prefix with  part and suffix with xml format"
        logs_info.append(log) 
        # creating a empty list for storing the part file that  prefix with  part and suffix with format name
        for i in list_f:
            filenames = i.name
            if filenames.startswith("part") and filenames.endswith("xml"):
                    list_of_file_name_starts_with_part_ends_with_xml.append(filenames)
                    log = f"{time} info {NOTEBOOK_PATH} Accessing prefix part file names and ends with xml format and stored in list "
                    logs_info.append(log) 
                    #appending file name  prefix with part file name and ends with format in list_of_file_name_starts_with_part
        length_of_part_file = len(list_of_file_name_starts_with_part_ends_with_xml)
        for j in range(length_of_part_file):
            old_file_path = structred_targetpath + "/" + list_of_file_name_starts_with_part_ends_with_xml[j]
            modified_name  = source_file_name_value
            new_file_path = structred_targetpath + "/" + modified_name 
            #print(new_file_path, old_file_path)
            dbutils.fs.mv(old_file_path, new_file_path)
    
    elif given_format == "parquet":
        file = pd.read_parquet(url_sas_token)
        log = f"{time} info {NOTEBOOK_PATH} Reading source file in parquet format {url_sas_token} "
        logs_info.append(log)
        # reading source file in source format
        df = spark.createDataFrame(file)
        log = f"{time} info {NOTEBOOK_PATH} Creating into dataframe "
        logs_info.append(log)
        #  creating  into dataframe
        sourceCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating sourceCount value {sourceCount} "
        logs_info.append(log)
        # calculating sourceCount value
        structred_targetpath = f"{targetpath_value}/{given_source_name}/{year}/{month}/{date}/{hours}"
        df.coalesce(1).write.format("parquet").mode("append").option("header",True).save(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Writing dataframe in target path in parquet format {structred_targetpath}"
        logs_info.append(log)
        # Writing dataframe in target path in parquet format
        TargetCount = df.count()
        log = f"{time} info {NOTEBOOK_PATH} Calculating taragetCount value {TargetCount} "
        logs_info.append(log)
        # calculating taragetCount value
        list_f = dbutils.fs.ls(structred_targetpath)
        log = f"{time} info {NOTEBOOK_PATH} Assigning  list of structred_targetpath to the variable"
        logs_info.append(log)
        # Assigning  list of structred_targetpath to the variable
        list_of_file_name_starts_with_part_ends_with_parquet = []
        
        log = f"{time} info {NOTEBOOK_PATH} creating a empty list for storing the part file that  prefix with  part and suffix with csv format"
        logs_info.append(log) 
        # creating a empty list for storing the part file that  prefix with  part and suffix with format name
        for i in list_f:
            filenames = i.name
            if filenames.startswith("part") and filenames.endswith("parquet"):
                    list_of_file_name_starts_with_part_ends_with_parquet.append(filenames)
                    log = f"{time} info {NOTEBOOK_PATH} Accessing prefix part file names and ends with csv format and stored in list "
                    logs_info.append(log) 
                    #appending file name  prefix with part file name and ends with format in list_of_file_name_starts_with_part
        length_of_part_file = len(list_of_file_name_starts_with_part_ends_with_parquet)
        for j in range(length_of_part_file):
            old_file_path = structred_targetpath + "/" + list_of_file_name_starts_with_part_ends_with_parquet[j]
            modified_name  = source_file_name_value
            new_file_path = structred_targetpath + "/" + modified_name 
            #print(new_file_path, old_file_path)
            dbutils.fs.mv(old_file_path, new_file_path)

    log = f"{time} info {NOTEBOOK_PATH} Reading file from {url_sas_token} and stored to {structred_targetpath}"
    logs_info.append(log)
    
    
    
except Exception as ex:
    time  = datetime.now()
    error_message = ex
    log = f"{time} info {NOTEBOOK_PATH} Error_message {error_message}"
    logs_info.append(log)
    # accessing error type 
    error_type = type(ex).__name__
    log = f"{time} info {NOTEBOOK_PATH} Error type {error_type}"
    logs_info.append(log)
    # accessing error type
    log = f"{time} info {NOTEBOOK_PATH}  Error occurred while reading file from files share write into adls containers, Error {error_message}"
    logs_info.append(log)
    # Error occurred while reading file from files share write into adls containers Error 
    
    Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, database_name =database_name, erro_run_stat_table_name = erro_run_stat_table_name, jdbcUsername = jdbcUsername, jdbcPassword = jdbcPassword)
    
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    
    dbutils.notebook.exit("Error occurred while reading file from fileshare and writing to adls")
    

############### generating end time and converting difference into seconds #######################
end_time = datetime.now()
duration = (end_time) - (start_time)
seconds = int(duration.total_seconds())    
RejectCount = int(sourceCount) - int(TargetCount)  
time = datetime.now()

################# Final Job_Run_Stats #################
log = f"{time} info {NOTEBOOK_PATH} Generating Final Job_Run_Stats"
logs_info.append(log)
log = f"{time} info {NOTEBOOK_PATH} Creating Job_Run_Stats schema for the Dataframe"
logs_info.append(log)
try:    
    Job_Run_Stats = [{"JobRunID": JobRunID,
                      "ExecutionRunID": f"{ExecutionRunID}",
                      "JobID": JobID,
                      "JobName": f"{JobName}",
                      "SourceSystem": f"NAS DRIVE", 
                      "SourcePath": f"{source_path}",
                      "TargetSystem": f"ADLS", 
                      "TargetPath": f"{structred_targetpath}",
                      "SourceCount": sourceCount,
                      "TargetCount":  TargetCount, 
                      "RejectCount": RejectCount, 
                      "JobStartTime": start_time, 
                      "JobEndTime": end_time, 
                      "DurationInSeconds": seconds, 
                      "Status": 'Completed'}]
        
    log = f"{time} info {NOTEBOOK_PATH} Dataframe creation and Datatype casting started"
    logs_info.append(log)
    tempdf2 = spark.createDataFrame(Job_Run_Stats)
    tempdf2 = tempdf2.withColumn("DurationInSeconds",tempdf2.DurationInSeconds.cast("int"))
    tempdf2 = tempdf2.withColumn("JobStartTime",to_timestamp("JobStartTime"))
    tempdf2 = tempdf2.withColumn("JobEndTime",to_timestamp("JobEndTime"))
    log = f"{time} info {NOTEBOOK_PATH} Dataframe created and Datatype casting completed"
    logs_info.append(log)
    log = f"{time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    tempdf2.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_run_stats_table_name}")\
              .option("user", f"{jdbcUsername}")\
              .option("password", f"{jdbcPassword}")\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    
    log = f"{time} info {NOTEBOOK_PATH} Generating Final Job_Run_Stats"
    logs_info.append(log)
    
    #display(tempdf2)
    # displaying final job run stats in output 
except Exception as ex:
    time = datetime.now()
    error_message = ex
    log = f"{time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    error_type = type(ex).__name__
    log = f"{time} info {NOTEBOOK_PATH} Error type {error_type}"
    logs_info.append(log)
    log = f"{time} info {NOTEBOOK_PATH} Error Occured while Generating Final Job_Run_Stats, Error {error_type} {error_message}"
    logs_info.append(log)
    # Error Occured while Generating Final Job_Run_Stats
    Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, database_name =database_name, erro_run_stat_table_name = erro_run_stat_table_name, jdbcUsername = jdbcUsername, jdbcPassword = jdbcPassword)
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error occurred while executing final job run stats")
    
    

############ Logs Generation #############

try:
    time = datetime.now()
    log = f"{time} info {NOTEBOOK_PATH} Converting the list of log messages into one string "
    logs_info.append(log)
    #  converting the list of log messages into one string
    log = f"{time} info {NOTEBOOK_PATH} Stored the log file in {log_path_latest} "
    logs_info.append(log)
    # stored the log file in adls
    string_log_info = "\n".join(logs_info)
    dbutils.fs.put(log_path_latest, string_log_info)
    #  stored the log file in text format. 
    
except Exception as ex:
    time = datetime.now()
    error_message = ex
    log = f"{time} info {NOTEBOOK_PATH} Error message {error_message}"
    logs_info.append(log)
    #  Error message
    error_type = type(ex).__name__
    log = f"{time} info {NOTEBOOK_PATH} Error type {error_type}"
    logs_info.append(log)
    #  error type
    log = f"{time} info {NOTEBOOK_PATH} Error occurred while log generation, Error {error_message}"
    logs_info.append(log)
    #print("Error occurred while log generation", error_message)
    
    log = f"{time} info {NOTEBOOK_PATH} Creating Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    # Creating Job_Error_Log Schema to create the Dataframe
 
    Error_log(current_time= time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, database_name =database_name, erro_run_stat_table_name = erro_run_stat_table_name, jdbcUsername = jdbcUsername, jdbcPassword = jdbcPassword)
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error occurred while executing Final Job_Run_Stats")
    


# COMMAND ----------

