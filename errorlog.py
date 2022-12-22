from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def Error_log(current_time, NOTEBOOK_PATH, error_message, log, error_type, ErrorID, ObjectRunID, ObjectName,
              ExecutionRunID,logs_info,log_path_latest,server_name,database_name,erro_run_stat_table_name,jdbcUsername,jdbcPassword):
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    data = [{"ErrorID": f'{ErrorID}',
             "ObjectRunID": f'{ObjectRunID}',
             "ObjectName": f'{ObjectName}',
             "ExecutionRunID": f'{ExecutionRunID}',
             "ErrorType": f'{error_type}',
             "ErrorCode": 500,
             "ErrorMessage": f'{error_message}',
             "CreatedDate": current_time}]
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Dataframe"
    logs_info.append(log)
    tempdf1 = spark.createDataFrame(data)
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Saving Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    spark.sql('TRUNCATE table table_error1')
    tempdf1.write.mode("append").saveAsTable("table_error1")
    log = f"{current_time} info {NOTEBOOK_PATH} Saved Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Dataframe from table"
    logs_info.append(log)
    table_to_dataframe = spark.sql('select * from table_error1')
    log = f"{current_time} info {NOTEBOOK_PATH} Created Dataframe from table"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Connection and started writing to SQL Database"
    logs_info.append(log)
    table_to_dataframe.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
              .option("dbtable", f"{erro_run_stat_table_name}")\
              .option("user", jdbcUsername)\
              .option("password",jdbcPassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    log = f"{current_time} info {NOTEBOOK_PATH} Created Connection and completed writing to SQL Database"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Generated Job_Error_Log"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Converting the list of log messages into one string "
    logs_info.append(log)
    #  converting the list of log messages into one string
    string_log_info = "\n".join(logs_info)
    dbutils.fs.put(log_path_latest, string_log_info)
   
    

