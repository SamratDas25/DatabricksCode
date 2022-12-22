# Databricks notebook source
#####importing packages
from datetime import datetime, timedelta
import pandas as pd
import pandas_profiling
from pandas_profiling import ProfileReport
import numpy as np
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

###### declare localvariables #####
targetCount = 0
time = datetime.now()
current_time = datetime.now()
ingest_date = current_time.strftime("%Y-%m-%d")
hours = current_time.strftime("%H")
date = current_time.strftime("%d")
month = current_time.strftime("%m")
year = current_time.strftime("%Y")

NOTEBOOK_PATH = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get())

start_time  = datetime.now() 
logs_info = []
log = f"{current_time} info {NOTEBOOK_PATH} Importing packages"
logs_info.append(log)

######################################################################

######## Import Widgets ################



dbutils.widgets.text("storage_account_name","SourceName","TargetName")
dbutils.widgets.text("JobRunID","JobName","JobId")
dbutils.widgets.text("TargetPath","ExecutionRunID","server_name")
dbutils.widgets.text("database_name","server_name","sourcetable_name")
dbutils.widgets.text("full_load","push_query","job_run_stats_dbtable_name")
dbutils.widgets.text("job_error_log_dbtable_name","ObjectRunID","ObjectName")
dbutils.widgets.text("target_container","ErrorID","BatchRunID")
dbutils.widgets.text("log_directory","BalanceAmountColumn","objectpath")
dbutils.widgets.text("sourcepath","filename","TableName")
dbutils.widgets.text("folder_name","db_scope")
###############################################################################

############# Loading Parameters #############################################

target_container = dbutils.widgets.get("target_container")
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
sourcetable_name = dbutils.widgets.get("sourcetable_name")
push_query = dbutils.widgets.get("push_query")
full_load = dbutils.widgets.get("full_load")    
ObjectRunID = dbutils.widgets.get("ObjectRunID")
BatchRunID = dbutils.widgets.get("BatchRunID")
log_directory = dbutils.widgets.get("log_directory")
BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")
objectpath = dbutils.widgets.get("objectpath")
filename = dbutils.widgets.get("filename")
sourcetable_name = dbutils.widgets.get("TableName")
folder_name = dbutils.widgets.get("folder_name")
db_scope = dbutils.widgets.get("db_scope")

################# Retreive Scope from Databricks Keyvault ############

try: 
    
    storagekey = dbutils.secrets.get(db_scope, key="strkey1")
    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storagekey)
    username = dbutils.secrets.get(db_scope, key = "dbuser")
    sqlpassword = dbutils.secrets.get(db_scope, key = "dbpass")
   
except Exception as e:
    print("Error retrieving Databricks KeyVault Credentials")
    print(e)
    
if len(storagekey) ==0: 
    raise Exception("Notebook job failed.. Please check Storage Key in Azure Key Vault")   

#####################################

try:
    JobRunID = dbutils.widgets.get("JobRunID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobRunID {JobRunID}"  
    logs_info.append(log)
    # Assigned JobRunID
    
    ExecutionRunID = dbutils.widgets.get("ExecutionRunID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ExecutionRunID {ExecutionRunID}"  
    logs_info.append(log)
    # Assigned BatchRunID
    
    JobID = dbutils.widgets.get("JobID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobID {JobID}"  
    logs_info.append(log)
    # Assigned JobID
    
    JobName = dbutils.widgets.get("JobName")
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobName {JobName}"  
    logs_info.append(log)
    # Assigned JobName
    
    SourceName = dbutils.widgets.get("SourceName")
    log = f"{time} info {NOTEBOOK_PATH} Assigned SourceName {SourceName}"  
    logs_info.append(log)
    # Assigned SourceName
    
    TargetName = dbutils.widgets.get("TargetName")
    log = f"{time} info {NOTEBOOK_PATH} Assigned TargetName {TargetName}"  
    logs_info.append(log)
    # Assigned TargetName

    database_name = dbutils.widgets.get("database_name")
    log = f"{time} info {NOTEBOOK_PATH} Assigned database_name {database_name}"  
    logs_info.append(log)
    # Assigned database_name
    
    job_error_log_name = dbutils.widgets.get("job_error_log_dbtable_name")
    log = f"{time} info {NOTEBOOK_PATH} Assigned dbtable_name {job_error_log_name}"  
    logs_info.append(log)
    
    
    job_run_stats_dbtable_name = dbutils.widgets.get("job_run_stats_dbtable_name")
    log = f"{time} info {NOTEBOOK_PATH} Assigned dbtable_name {job_run_stats_dbtable_name}"  
    logs_info.append(log)
    # Assigned dbtable_name
   
       
    ErrorID = dbutils.widgets.get("ErrorID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ErrorID {ErrorID}"  
    logs_info.append(log)
    # Assigned ErrorID
    
    ObjectRunID = dbutils.widgets.get("ObjectRunID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ObjectRunID {ObjectRunID}"  
    logs_info.append(log)
    # Assigned ObjectRunID
    
    ObjectName = dbutils.widgets.get("ObjectName")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ObjectName {ObjectName}"  
    logs_info.append(log)    
    log = f"{current_time} info {NOTEBOOK_PATH} Assigned values which were imported from parameter file"
    logs_info.append(log)
    
    job_error_log_name = dbutils.widgets.get("job_error_log_dbtable_name")
    log = f"{time} info {NOTEBOOK_PATH} Assigned dbtable_name {job_error_log_name}"  
    logs_info.append(log)
    logfilename = folder_name
    log_target = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/Log_directory/{year}/{month}/{date}/{hours}"
    log_path_latest =log_target +'/'+ "log_SQL_to_ADLS_" + logfilename + "_" +str(current_time) +" log.txt"
       
    
except Exception as ex:
    current_time = datetime.now()
    logfilename = folder_name
    log_target = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/Log_directory/{year}/{month}/{date}/{hours}"
    log_path_latest =log_target +'/'+ "log_SQL_to_ADLS_" + logfilename + "_" +str(current_time) +" log.txt" 
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while assigning values from parameter files"
    logs_info.append(log)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,username,sqlpassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred")

###### Fetch data based on job name ###########

querydata = "select JobId from metaabc.Job_Metadata where JobName ="+"'"+JobName+"'"

JobId_df = (spark.read
  .format("jdbc")\
  .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}")\
  .option("query", querydata)\
  .option("user", username)\
  .option("password", sqlpassword)\
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
  .load()
)

#print(JobId_df)
JobId=JobId_df.collect()[0][0]

log = f"{current_time} info {NOTEBOOK_PATH} Calling wrapper function by passing Sourcepath, filename, BalanceAmountColumn"
logs_info.append(log)
# Calling wrapper function by passing Sourcepath, filename, BalanceAmountColumn
ObjectType = 'SQLTable'
validationResults = wrapperFunction(JobId, objectpath, filename, BalanceAmountColumn,"SQLTable","",server_name,database_name)

log = f"{current_time} info {NOTEBOOK_PATH} Generated Job Rule Validation"
logs_info.append(log)
# Generated Job Rule Validation 

log = f"{current_time} info {NOTEBOOK_PATH} Validation Results : {validationResults}"
logs_info.append(log)
# Log with Rule Validation Results 
    
######### loading config file #########
log = f"{current_time} info {NOTEBOOK_PATH} Reading the CSV Parameter File"
logs_info.append(log)

    
####### reading file from SQL server
try: 
    df = spark.read.format("jdbc") \
     .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
     .option("dbtable",f"{sourcetable_name}") \
     .option("user", username) \
     .option("password", sqlpassword) \
     .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
     .load()

### writing to source parquet in adls ######                             					  
 
  # calculating sourceCount value 
    sourceCount = df.count()
    log = f"{current_time} info {NOTEBOOK_PATH} Calculating Source Count "
    logs_info.append(log)
       
    log = f"{current_time} info {NOTEBOOK_PATH} File loaded to Dataframe"
    logs_info.append(log)
    
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Loading file to dataframe"
    logs_info.append(log)
    Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)

################ writing file to the target location #####################
log = f"{current_time} info {NOTEBOOK_PATH} Started writing to Target Location"
logs_info.append(log)

tg_path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{folder_name}/"

try:    
    current_time = datetime.now()
    #standard_df.write.mode("append").parquet(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/")
    df.write.mode("append").parquet(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{year}/{month}/{date}/{hours}")
    log = f"{current_time} info {NOTEBOOK_PATH} Writing to Target Location Completed"
    logs_info.append(log)
    
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while writing to Target Location"
    logs_info.append(log)

    ############ Generating Job_Run_Stats #################
log = f"{current_time} info {NOTEBOOK_PATH} Generating Initial Job_Run_Stats"
logs_info.append(log)
log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Run_Stats schema for the Dataframe"
logs_info.append(log)
try:
    current_time = datetime.now()
    Job_Run_Stats = [{"JobRunID": JobRunID,
                      "ExecutionRunID": f"{ExecutionRunID}",
                      "JobID": JobID,
                      "JobName": f"{JobName}",
                      "SourceSystem": f"SQL", 
                      "SourcePath": f"{sourcetable_name}",
                      "TargetSystem": f"ADLS", 
                      "TargetPath": f"{tg_path}",
                      "SourceCount": 0,
                      "TargetCount":  0, 
                      "RejectCount": 0, 
                      "JobStartTime": start_time, 
                      "JobEndTime": 0, 
                      "DurationInSeconds": 0, 
                      "Status": 'Started'}]
    log = f"{current_time} info {NOTEBOOK_PATH} Job_Run_Stats schema has been created"
    logs_info.append(log)
    
    log = f"{current_time} info {NOTEBOOK_PATH} Dataframe creation and Datatype casting started"
    logs_info.append(log)
    # log comment
    jobdf2 = spark.createDataFrame(Job_Run_Stats)
    jobdf2 = jobdf2.withColumn("DurationInSeconds",jobdf2.DurationInSeconds.cast("int"))
    jobdf2 = jobdf2.withColumn("JobStartTime",to_timestamp("JobStartTime"))
    jobdf2 = jobdf2.withColumn("JobEndTime",to_timestamp("JobEndTime"))
    
    log = f"{current_time} info {NOTEBOOK_PATH} Dataframe created and Datatype casting completed"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
            
    jobdf2.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_run_stats_dbtable_name}")\
              .option("user", username)\
              .option("password", sqlpassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    #log comment
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Initial Job_Run_Stats"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while generating initial job run stats"
    
try:
    
    df = spark.read.format("jdbc") \
         .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
         .option("dbtable",f"{sourcetable_name}") \
         .option("user", username) \
         .option("password", sqlpassword) \
         .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
         .load()                     
    sourceCount = df.count()
    log = f"{time} info {NOTEBOOK_PATH} Calculating Source Count "
    logs_info.append(log)

    log = f"{current_time} info {NOTEBOOK_PATH} File loaded to Dataframe"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Loading file to dataframe"
    logs_info.append(log)
    
    Error_log(current_time= current_time,NOTEBOOK_PATH= NOTEBOOK_PATH,error_message= error_message,log= log,error_type= error_type,ErrorID= ErrorID,ObjectRunID=   ObjectRunID,ObjectName= ObjectName,ExecutionRunID= ExecutionRunID,logs_info = logs_info, log_path_latest = log_path_latest, server_name = server_name, database_name =database_name, erro_run_stat_table_name = job_error_log_dbtable_name, jdbcUsername = username, jdbcPassword = sqlpassword)
    
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    
    dbutils.notebook.exit("Error occurred while reading file from fileshare and writing to adls")
        
###################### Job_Error_Log ##########################
log = f"{current_time} info {NOTEBOOK_PATH} Generating Job_Error_Log"
logs_info.append(log)
try:
    current_time = datetime.now()
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Dataframe from table"
    logs_info.append(log)
    table_to_dataframe = spark.sql('select * from error_temp1')
    
    log = f"{current_time} info {NOTEBOOK_PATH} Created Dataframe from table"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    table_to_dataframe.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_error_log_dbtable_name}")\
              .option("user", username)\
              .option("password", sqlpassword)\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    spark.sql('TRUNCATE table error_temp1')
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Job_Error_Log"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    log = f"{current_time} info {NOTEBOOK_PATH} Error Occured while Generating Job_Error_Log"
    logs_info.append(log)
    error_message = ex
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    data = [{"ErrorID": f'{ErrorID}',
             "ObjectRunID": f'{ObjectRunID}',
             "ObjectName": f'{ObjectName}',
             "ExecutionRunID": f'{ExecutionRunID}',
             "ErrorType": f'{error_type}',
             "ErrorCode": 404,
             "ErrorMessage": f'{ex}'}]
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Dataframe"
    logs_info.append(log)
    tempdf1 = spark.createDataFrame(data)
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Saving Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    tempdf1.write.mode("append").saveAsTable("error_temp1")
    log = f"{current_time} info {NOTEBOOK_PATH} Saved Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Generating Job_Error_Log"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Dataframe from table"
    logs_info.append(log)
    table_to_dataframe = spark.sql('select * from error_temp1')
    log = f"{current_time} info {NOTEBOOK_PATH} Created Dataframe from table"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    table_to_dataframe.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_error_log_name}")\
              .option("user", username)\
              .option("password", sqlpassword)\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    spark.sql("TRUNCATE table error_temp1")
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Job_Error_Log"
    logs_info.append(log)
    
##################### generating logs ###########################
current_time = datetime.now()
current_time = datetime.now()
ingest_date = current_time.strftime("%Y-%m-%d")
hours = current_time.strftime("%H")
date = current_time.strftime("%d")
month = current_time.strftime("%m")
year = current_time.strftime("%Y")
#print (log_path_latest)

try:
    log = f"{current_time} info {NOTEBOOK_PATH} reading the log info and saving it in Log Directory"
    logs_info.append(log)
    string_log_txt = "\n".join(logs_info)
    dbutils.fs.put(log_path_latest, string_log_txt)
except Exception as ex:
    print('Exception:')
    print(ex)     
    print("Error occured appending logs") 
    
############ reading data based on input parameters #################

if full_load == "Y":
    jdbcDF = spark.read.format("jdbc") \
         .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
         .option("dbtable",f"{sourcetable_name}") \
         .option("user", username) \
         .option("password", sqlpassword) \
         .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
         .load()

    targetCount = jdbcDF.count()
    path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{folder_name}/{year}/{month}/{date}/{hours}"
    jdbcDF.write.mode("append").parquet(path)

else:
    print("Full Load Value is No")
    print("Proceeding for incremental Load")
########For incremental load########
    df1 = spark.read.option("header","true").parquet(path) 
    df2 = df1.agg({'ModifiedDate': 'max'})
 ######Load data based on selected date########
max_data = spark.read.format("jdbc") \
     .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
     .option("query", push_query) \
     .option("user", username) \
     .option("password", sqlpassword) \
     .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
     .load()  

#######Query for loading specific columns from a table######
###spec_query = 'Select FirstName, LastName, CompanyName from '+ sourcetable_name+''

#spec_data = spark.read.format("jdbc") \
#     .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
#     .option("query", spec_query) \
#     .option("user", username) \
#     .option("password", sqlpassword) \
#     .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#     .load()  
   
############### generating end time and converting difference into seconds #######################
end_time = datetime.now()
duration = (end_time) - (start_time)
seconds = int(duration.total_seconds())
    
    
##########################################################################################################
################# Final Job_Run_Stats #################

log = f"{current_time} info {NOTEBOOK_PATH} Calculating target count"
logs_info.append(log)
reject_count = int(sourceCount)-int(targetCount)
log = f"{current_time} info {NOTEBOOK_PATH} calculating reject count"
logs_info.append(log)   
log = f"{current_time} info {NOTEBOOK_PATH} Generating Final Job_Run_Stats"
logs_info.append(log)
log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Run_Stats schema for the Dataframe"
logs_info.append(log)
try:
    current_time = datetime.now()
    Job_Run_Stats = [{"JobRunID": JobRunID,
                      "ExecutionRunID": f"{ExecutionRunID}",
                      "JobID": JobID,
                      "JobName": f"{JobName}",
                      "SourceSystem": f"SQL", 
                      "SourcePath": f"{sourcetable_name}",
                      "TargetSystem": f"ADLS", 
                      "TargetPath": f"{tg_path}",
                      "SourceCount": sourceCount,
                      "TargetCount":  targetCount, 
                      "RejectCount": reject_count, 
                      "JobStartTime": start_time, 
                      "JobEndTime": end_time, 
                      "DurationInSeconds": seconds, 
                      "Status": 'Completed'}]
    log = f"{current_time} info {NOTEBOOK_PATH} Job_Run_Stats schema has been created"
    logs_info.append(log)
    
    log = f"{current_time} info {NOTEBOOK_PATH} Dataframe creation and Datatype casting started"
    logs_info.append(log)
    tempdf2 = spark.createDataFrame(Job_Run_Stats)
    tempdf2 = tempdf2.withColumn("DurationInSeconds",tempdf2.DurationInSeconds.cast("int"))
    tempdf2 = tempdf2.withColumn("JobStartTime",to_timestamp("JobStartTime"))
    tempdf2 = tempdf2.withColumn("JobEndTime",to_timestamp("JobEndTime"))
    log = f"{current_time} info {NOTEBOOK_PATH} Dataframe created and Datatype casting completed"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    tempdf2.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{job_run_stats_dbtable_name}")\
              .option("user", username)\
              .option("password", sqlpassword)\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    
    log = f"{current_time} info {NOTEBOOK_PATH} Generating Final Job_Run_Stats"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    log = f"{current_time} info {NOTEBOOK_PATH} Error Occured while Generating Final Job_Run_Stats"
    logs_info.append(log)
    error_message = ex
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    data = [{"ErrorID": f'{ErrorID}',
             "ObjectRunID": f'{ObjectRunID}',
             "ObjectName": f'{ObjectName}',
             "ExecutionRunID": f'{ExecutionRunID}',
             "ErrorType": f'{error_type}',
             "ErrorCode": 404,
             "ErrorMessage": f'{ex}'}]
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Dataframe"
    logs_info.append(log)
    tempdf1 = spark.createDataFrame(data)
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Saving Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    tempdf1.write.mode("append").saveAsTable("error_temp1")
    log = f"{current_time} info {NOTEBOOK_PATH} Saved Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    
    
    
  