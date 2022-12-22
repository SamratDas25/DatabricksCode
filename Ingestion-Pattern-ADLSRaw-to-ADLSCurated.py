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

###Parameters###
dbutils.widgets.text("storage_account_name","filename","sourcepath")
dbutils.widgets.text("source_container","target_container","JobRunID")
dbutils.widgets.text("JobID","JobName")
dbutils.widgets.text("tablename","ErrorID","ObjectRunID")
dbutils.widgets.text("adls2adls_scope","storage_account_key")
dbutils.widgets.text("database_name","job_error_log_dbtable_name")
dbutils.widgets.text("server_name","job_run_stats_dbtable_name")
dbutils.widgets.text("ExecutionRunID","ObjectName")
dbutils.widgets.text("log_directory","BalanceAmountColumn")
###End###
NOTEBOOK_PATH = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get())

current_time = datetime.now()
start_time  = datetime.now() 

logs_info = []

log = f"{current_time} info {NOTEBOOK_PATH} Importing packages"
logs_info.append(log)

################ configuring Storage Account ################
log = f"{current_time} info {NOTEBOOK_PATH} Configuring the Storage Account"
logs_info.append(log)
try:
    current_time = datetime.now()
    spark.conf.set(f"fs.azure.account.key.adlsrawtoadlscurated.dfs.core.windows.net",dbutils.secrets.get("dbscope2","storageaccountkey1"))
    jdbcUsername = dbutils.secrets.get(scope = "adls2adlsscope", key = "SQLUser")
    jdbcPassword = dbutils.secrets.get(scope = "adls2adlsscope", key = "SQLPassword")
    log = f"{current_time} info {NOTEBOOK_PATH} Configured the Storage Account"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Configuring the Storage Account"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    
##### validation checks #####  

log = f"{current_time} info {NOTEBOOK_PATH} Creating a variables for Job Rule Validation"
logs_info.append(log)
# Creating a variables for Job Rule Validation

filename = dbutils.widgets.get("filename")
sourcefilename = filename #variable used to pick the file from source

### filename extraction ###

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

sourcepath = dbutils.widgets.get("sourcepath")
BalanceAmountColumn = dbutils.widgets.get("BalanceAmountColumn")
objectpath = dbutils.widgets.get("objectpath")
server_name = dbutils.widgets.get("server_name")
database_name = dbutils.widgets.get("database_name")
JobName = dbutils.widgets.get("JobName")

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
ObjectType = 'File'

# Calling wrapper function by passing Sourcepath, filename, BalanceAmountColumn
validationResults = wrapperFunction(JobId, objectpath, filename, BalanceAmountColumn,"File","",server_name,database_name)

log = f"{current_time} info {NOTEBOOK_PATH} Generated Job Rule Validation"
logs_info.append(log)
# Generated Job Rule Validation 

log = f"{current_time} info {NOTEBOOK_PATH} Validation Results : {validationResults}"
logs_info.append(log)
# Log with Rule Validation Results 
    
######### loading config file #########
log = f"{current_time} info {NOTEBOOK_PATH} Reading the CSV Parameter File"
logs_info.append(log)
try:
    time = datetime.now()
    log = f"{current_time} info {NOTEBOOK_PATH} Assigning values which are being imported from parameter file"
    logs_info.append(log)

    current_time = datetime.now()  
    #variable_df = config.toPandas()
    #variable_list = variable_df.set_index('Variable_names').to_dict(orient='index')
    
    storage_account_name = dbutils.widgets.get("storage_account_name")
   
    source_container = dbutils.widgets.get("source_container")
   
    target_container = dbutils.widgets.get("target_container")
    log_directory = dbutils.widgets.get("log_directory")
    
    JobRunID = dbutils.widgets.get("JobRunID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobRunID {JobRunID}"  
    logs_info.append(log)
    # Assigned JobRunID
    
    ExecutionRunID = dbutils.widgets.get("ExecutionRunID")
    log = f"{time} info {NOTEBOOK_PATH} Assigned ExecutionRunID {ExecutionRunID}"  
    logs_info.append(log)
    # Assigned BatchRunID
    
    JobID = dbutils.widgets.get("JobID")
    JobID = JobId
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobID {JobID}"  
    logs_info.append(log)
    # Assigned JobID
      
    log = f"{time} info {NOTEBOOK_PATH} Assigned JobName {JobName}"  
    logs_info.append(log)
    # Assigned JobName
    
    
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
    
    now=datetime.now()
    d = now.strftime("%Y%m%d")
    t = now.strftime("%H%M%S")
    currenttimestamp=str(d) +"T"+ str(t)
    log_target = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/Log_directory/{year}/{month}/{date}/{hours}"
    log_path_latest =log_target +'/'+ "log_ADLSRaw_to_ADLSCurated_" + logfilename + "_" + currenttimestamp +" log.txt"
    
    
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while assigning values from parameter files"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit()

#####
sc_path = f"abfss://{source_container}@{storage_account_name}.dfs.core.windows.net/{sourcepath}/{year}/{month}/{date}/{hours}/{sourcefilename}"
tg_path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{given_source_name}/{year}/{month}/{date}/{hours}"
    
############# Generating Job_Run_Stats #################
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
                      "SourceSystem": f"ADLS", 
                      "SourcePath": f"{sc_path}",
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
              .option("user", jdbcUsername)\
              .option("password",jdbcPassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)    
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Initial Job_Run_Stats"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Generating Initial Job_Run_Stats"
    logs_info.append(log)
#     Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
#### Objectpath = f"abfss://{source_container}@{storage_account_name}.dfs.core.windows.net/{sourcepath}/{filename}"
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while Generating Initial Job_Run_Stats")


### reading file from ADLS container

# split_source_name = filename.split('.')
# given_source_name = split_source_name[0]
# given_format = split_source_name[-1]

sc_path = f"abfss://{source_container}@{storage_account_name}.dfs.core.windows.net/{sourcepath}/{year}/{month}/{date}/{hours}/{sourcefilename}"

try:
    current_time = datetime.now()  
    df = spark.read.load(path = f"abfss://{source_container}@{storage_account_name}.dfs.core.windows.net/{sourcepath}/{year}/{month}/{date}/{hours}/{sourcefilename}",
                         format=f"{given_format}", header = True)
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
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error occurred While Reading file from ADLS container")

#performing de-duplication    
log = f"{current_time} info {NOTEBOOK_PATH} Performing De-Duplication"
logs_info.append(log)

try:
    current_time = datetime.now()
    df.createOrReplaceTempView("temp_sql")
    #verify deduplication logic with the team
    distinct_df = spark.sql("""select distinct * from temp_sql""")
    log = f"{current_time} info {NOTEBOOK_PATH} Performed De-Duplication"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Performing De-Duplication"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while performing de-duplication ")
    
#################### performing standardization #######################
log = f"{current_time} info {NOTEBOOK_PATH} Performing Standardization"
logs_info.append(log)

try:
    current_time = datetime.now()
    standard_df = distinct_df.withColumn("HIRE_DATE",date_format(to_date(col("HIRE_DATE"), "dd-MM-yy"), "MM-dd-yyyy"))
    log = f"{current_time} info {NOTEBOOK_PATH} Performed Standardization"
    logs_info.append(log)
    targetCount  = standard_df.count()
    log = f"{current_time} info {NOTEBOOK_PATH} Calculating target count"
    logs_info.append(log)
    reject_count = int(sourceCount)-int(targetCount)
    log = f"{current_time} info {NOTEBOOK_PATH} calculating reject count"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Performing Standardization"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while Performing Standardization ")
    
######################## generating Data Profile Report ###################################
log = f"{current_time} info {NOTEBOOK_PATH} Generating Data Profile Report"
logs_info.append(log)
try:
    current_time = datetime.now()  
    data_profiling = standard_df.select("*").toPandas()
    profile_report = displayHTML(pandas_profiling.ProfileReport(data_profiling).html)
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Data Profile Report"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Generating Data Profile Report"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while Generating Data Profile Report ")
    
################# writing file to the target location #####################
log = f"{current_time} info {NOTEBOOK_PATH} Started writing to Target Location"
logs_info.append(log)

# current_time = datetime.now()
# ingest_date = current_time.strftime("%Y-%m-%d")
# hours = current_time.strftime("%H")
# date = current_time.strftime("%d")
# month = current_time.strftime("%m")
# year = current_time.strftime("%Y")


#tg_path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{year}/{month}/{date}/{hours}"
tg_path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{given_source_name}/{year}/{month}/{date}/{hours}"

try:    
    current_time = datetime.now()
    #standard_df.write.mode("append").parquet(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/"
    standard_df.write.mode("append").parquet(f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/{given_source_name}/{year}/{month}/{date}/{hours}")
    log = f"{current_time} info {NOTEBOOK_PATH} Writing to Target Location Completed"
    logs_info.append(log)
    
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while writing to Target Location"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while Writing to adls container ")
    
############### generating end time and converting difference into seconds #######################
end_time = datetime.now()
duration = (end_time) - (start_time)
seconds = int(duration.total_seconds())

################# Final Job_Run_Stats #################
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
                      "SourceSystem": f"ADLS", 
                      "SourcePath": f"{sc_path}",
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
              .option("user", jdbcUsername)\
              .option("password",jdbcPassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Final Job_Run_Stats"
    logs_info.append(log)
except Exception as ex:
    current_time = datetime.now()
    error_message = ex
    error_type = type(ex).__name__
    log = f"{current_time} info {NOTEBOOK_PATH} Error occured while Generating Final Job_Run_Stats"
    logs_info.append(log)
    #Error_log(current_time = current_time, NOTEBOOK_PATH = NOTEBOOK_PATH, error_message = error_message, log = log, error_type = error_type, ErrorID = ErrorID, ObjectRunID = ObjectRunID, ObjectName = ObjectName, ExecutionRunID = ExecutionRunID)
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while Generating Final Job_Run_Stats ")

    
##################### generating logs ###########################
# current_time = datetime.now()
# log_target = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/"
# log_path_latest = log_target +'/'+f"{log_directory}"+'/'+ str(current_time) +" log.txt" 


# ingest_date = current_time.strftime("%Y-%m-%d")
# hours = current_time.strftime("%H")
# date = current_time.strftime("%d")
# month = current_time.strftime("%m")
# year = current_time.strftime("%Y")
now=datetime.now()
d = now.strftime("%Y%m%d")
t = now.strftime("%H%M%S")
currenttimestamp=str(d) +"T"+ str(t)
log_target = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/Log_directory/{year}/{month}/{date}/{hours}"
log_path_latest =log_target +'/'+ "log_ADLSRaw_to_ADLSCurated_" + logfilename + "_" + currenttimestamp +" log.txt"


try:
    log = f"{current_time} info {NOTEBOOK_PATH} reading the log info and saving it in Log Directory"
    logs_info.append(log)
    string_log_txt = "\n".join(logs_info)
    dbutils.fs.put(log_path_latest, string_log_txt)
except Exception as ex:
    print('Exception:')
    print(ex)     
    print("Error occured appending logs")
    Error_log(current_time,NOTEBOOK_PATH,error_message,log,error_type,ErrorID,ObjectRunID,ObjectName,ExecutionRunID,logs_info,log_path_latest,server_name,database_name, job_error_log_name,jdbcUsername,jdbcPassword)
    spark.sql('TRUNCATE table table_error1')
    log = f"{time} info {NOTEBOOK_PATH} Calling Errorlog module to store Error type, Error message & logs"
    logs_info.append(log)
    #Calling Errorlog module to store Error type, Error message & logs
    log = f"{time} info {NOTEBOOK_PATH} Notebook Exited"
    logs_info.append(log)
    dbutils.notebook.exit("Error Occurred while storing the log file in ADLS")