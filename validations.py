from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from datetime import datetime, timedelta

from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

userinfo = NOTEBOOK_PATH.split('/')
Username = userinfo[2]



def wrapperFunction(JobId, ObjectPath, ObjectName, BalanceAmountColumn, ObjectType, ObjectPath_PostProcessing,server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    Querydata = 'Select * From abc.Job_Rule_Assignment Where Job_Id = ' + str(JobId)

    JobRuleDetails_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", Querydata) \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    # Rule_df = Rule_df.filter(Rule_df.Rule_Name == RuleName)
    JobRuleDetails_pdf = JobRuleDetails_df.toPandas()
    Output = str('')
    # print(JobRuleDetails_pdf)
    for ind in JobRuleDetails_pdf.index:
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 1):
            Out1 = CheckJobRule_CountValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name)
            Output = Output + Out1 + " | " 
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 2):
            Out2 = CheckJobRule_BalanceAmountValidation(JobId, ObjectPath, ObjectName, BalanceAmountColumn, ObjectType,server_name,database_name)
            Output = Output + Out2 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 3):
            Out3 = CheckJobRule_ThresholdValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name)
            Output = Output + Out3 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 4):
            Out4 = CheckJobRule_FileNameValidation(JobId, ObjectPath, ObjectName, server_name,database_name)
            Output = Output + Out4 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 5):
            Out5 = CheckJobRule_FileSizeValidation(JobId, ObjectPath, ObjectName, server_name,database_name)
            Output = Output + Out5 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 6):
            Out6 = CheckJobRule_FileArrival_Validation(JobId, ObjectPath, ObjectName, server_name,database_name)
            Output = Output + Out6 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 7):
            Out7 = CheckJobRule_Missing_FileCheck_Validation(JobId, ObjectPath, ObjectName, server_name,database_name)
            Output = Output + Out7 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 8):
            Out8 = CheckJobRule_CountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName, server_name,database_name)
            Output = Output + Out8 + " | "
        if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 9):
            Out9 = CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName,
                                                                           BalanceAmountColumn, server_name,database_name)
            Output = Output + Out9 + " "

    # return out1+out2+out3+out4+out5+out6+out7+out8+out8+out9
    return Output


def CheckJobRule_CountValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(1)
    
    given_name =ObjectName.split(".")
    given_format = given_name[-1] 
    
    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    Src_Count = int(AuditTableFileCount_pdf['File_Count'])

    if (ObjectType == 'File'):
        spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                       dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                       )
        #RawLogData_df = spark.read.csv(path=ObjectPath + ObjectName, header=True)
        RawLogData_df = spark.read.load(path=ObjectPath + ObjectName, format=f"{given_format}", header = True)
        Tgt_Count = int(RawLogData_df.count())
    if (ObjectType == 'SQLTable'):
        Querydata = 'Select * From ' + str(ObjectName)
        SQLTable_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
            .option("query", Querydata) \
            .option("user", username) \
            .option("password", sqlpassword) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        Tgt_Count = int(SQLTable_df.count())
    if (Src_Count == Tgt_Count):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'

    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": Src_Count,
                                    "Target_Value": Tgt_Count,
                                    "Source_Value_Type": 'Source Count',
                                    "Target_Value_Type": 'Target Count',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    if (RuleStatus == 'Passed'):
        out = str('CountValidation passed')
    else:
        out = str('CountValidation Failed')

    return out


def CheckJobRule_BalanceAmountValidation(JobId, ObjectPath, ObjectName, BalanceAmountColumn, ObjectType,server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(2)
    
    given_name =ObjectName.split(".")
    given_format = given_name[-1]
    
    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    SourceValue = int(AuditTableFileCount_pdf['File_Balance_Amount'])

    # query2 = 'Select BalanceAmountColumn from metaabc.File_MetaData where = ' + str(ObjectName)
    FileMetaData_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select FileName,BalanceAmountColumn from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    FileMetaDataFilter_df = FileMetaData_df.filter(FileMetaData_df.FileName == ObjectName)
    BalanceAmountColumn1 = FileMetaDataFilter_df.collect()[0][1]

    if (ObjectType == 'File'):
        spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                       dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                       )
        #TargetData_df = spark.read.csv(path=ObjectPath + ObjectName, header=True)
        TargetData_df = spark.read.load(path=ObjectPath + ObjectName, format=f"{given_format}", header = True)            
        BalanceAmount = TargetData_df.agg({BalanceAmountColumn: 'sum'})
        for col in BalanceAmount.dtypes:
            TargetValue = int(BalanceAmount.first()[col[0]])
    if (ObjectType == 'SQLTable'):
        Querydata = 'Select * From ' + str(ObjectName)
        SQLTable_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
            .option("query", Querydata) \
            .option("user", username) \
            .option("password", sqlpassword) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        BalanceAmount = SQLTable_df.agg({BalanceAmountColumn: 'sum'})
        for col in BalanceAmount.dtypes:
            TargetValue = int(BalanceAmount.first()[col[0]])

    if (SourceValue == TargetValue):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Source Balance Amount',
                                    "Target_Value_Type": 'Target Balance Amount',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'Passed'):
        out = str('BalanceAmountValidation passed')
    else:
        out = str('BalanceAmountValidation Failed')

    return out


def CheckJobRule_ThresholdValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(3)
    
    given_name =ObjectName.split(".")
    given_format = given_name[-1]

    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    SourceValue_min = FileMetadata_pdf.loc[0]['MinValue']
    SourceValue_max = FileMetadata_pdf.loc[0]['MaxValue']
    SourceValue = 'MinValue = ' + str(SourceValue_min) + ' and MaxValue = ' + str(SourceValue_max)

    if (ObjectType == 'File'):
        spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                       dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                       )
        #RawLogData_df = spark.read.csv(path=ObjectPath + ObjectName, header=True)
        RawLogData_df = spark.read.load(path=ObjectPath + ObjectName, format=f"{given_format}", header = True)
        TargetValue = int(RawLogData_df.count())
    if (ObjectType == 'SQLTable'):
        SQLTable_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
            .option("query", "Select * from SalesLT.Customer") \
            .option("user", username) \
            .option("password", sqlpassword) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        TargetValue = int(SQLTable_df.count())

    if (TargetValue >= SourceValue_min and TargetValue <= SourceValue_max):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Source Threshold Value',
                                    "Target_Value_Type": 'Target Object Count',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'FileMetaDataTable',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'Passed'):
        out = 'ThresholdValidation passed'
    else:
        out = 'ThresholdValidation Failed'

    return out


def CheckJobRule_FileNameValidation(JobId, ObjectPath, ObjectName, server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(4)
    
    given_name =ObjectName.split(".")
    given_format = given_name[-1]

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTable_df = int(AuditTable_df.filter(AuditTable_df.File_Name == ObjectName).count())
    if (AuditTable_df > 0):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'

    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": ObjectName,
                                    "Target_Value": ObjectName,
                                    "Source_Value_Type": 'Object Name',
                                    "Target_Value_Type": 'Object Name',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'FileMetaDataTable',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))

    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'Passed'):
        out = 'FileNameValidation passed'
    else:
        out = 'FileNameValidation Failed'

    return out


def CheckJobRule_FileSizeValidation(JobId, ObjectPath, ObjectName, server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(5)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    SourceValue = AuditTableFileCount_pdf.loc[0]['File_Size']

    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    TargetValue = FileMetadata_pdf.loc[0]['FileSize']

    if (SourceValue == TargetValue):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Source Object Size',
                                    "Target_Value_Type": 'Target Object Size',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'FileMetaDataTable',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'Passed'):
        out5 = str('File Size Validation passed')
    else:
        out5 = str('File Size Validation Failed')

    return out5


def CheckJobRule_Missing_FileCheck_Validation(JobId, ObjectPath, ObjectName, server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(7)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    # display(AuditTable_df)

    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    Source_FileNameValue = AuditTableFileCount_pdf.loc[0]['File_Name']
    FileArrival_Time = AuditTableFileCount_pdf.loc[0]['File_Arrival_Time']
    # print(FileArrival_Time,type(FileArrival_Time))
    File_Arrival_Time = FileArrival_Time.strftime("%H:%M:%S")

    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    # display(FileMetadata_pdf)
    Target_FileNameValue = FileMetadata_pdf.loc[0]['FileName']
    File_Expected_Time = FileMetadata_pdf.loc[0]['File_Expected_Time']
    File_Buffer_Time = FileMetadata_pdf.loc[0]['File_Buffer_Time']

    File_Buffer_Time = File_Buffer_Time / 60
    # print(File_Expected_Time)
    Postive_Buffer_Time = File_Expected_Time + timedelta(hours=File_Buffer_Time)
    Postive_Buffer_Time = Postive_Buffer_Time.strftime("%H:%M:%S")
    # print(Postive_Buffer_Time)

    Negative_Buffer_Time = File_Expected_Time - timedelta(hours=File_Buffer_Time)
    Negative_Buffer_Time = Negative_Buffer_Time.strftime("%H:%M:%S")
    # print(Negative_Buffer_Time)

    if (Source_FileNameValue == Target_FileNameValue) and (
            Negative_Buffer_Time < File_Arrival_Time < Postive_Buffer_Time):
        RuleStatus = "Passed"
    else:
        RulesStatus = "Failed"

    Created_Time = datetime.now()
    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": File_Arrival_Time,
                                    "Target_Value": f"{File_Arrival_Time} present in between {Negative_Buffer_Time}, {Postive_Buffer_Time}",
                                    "Source_Value_Type": 'Source Object Arrival Time',
                                    "Target_Value_Type": 'Target Expected Time',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'Passed'):
        out = 'Missing_FileCheck_Validation passed'
    else:
        out = 'Missing_FileCheck_Validation Failed'

    return out


def CheckJobRule_FileArrival_Validation(JobId, ObjectPath, ObjectName, server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(6)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    # display(AuditTable_df)

    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    Source_FileNameValue = AuditTableFileCount_pdf.loc[0]['File_Name']
    FileArrival_Time = AuditTableFileCount_pdf.loc[0]['File_Arrival_Time']
    # print(FileArrival_Time,type(FileArrival_Time))
    File_Arrival_Time = FileArrival_Time.strftime("%H:%M:%S")
    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    # display(FileMetadata_pdf)
    Target_FileNameValue = FileMetadata_pdf.loc[0]['FileName']
    File_Expected_Time = FileMetadata_pdf.loc[0]['File_Expected_Time']
    File_Buffer_Time = FileMetadata_pdf.loc[0]['File_Buffer_Time']
    File_Expected_Time_tt = File_Expected_Time.strftime("%H:%M:%S")
    File_Buffer_Time = File_Buffer_Time / 60
    # print(File_Expected_Time)
    Postive_Buffer_Time = File_Expected_Time + timedelta(hours=File_Buffer_Time)
    Postive_Buffer_Time = Postive_Buffer_Time.strftime("%H:%M:%S")
    # print(Postive_Buffer_Time)

    Negative_Buffer_Time = File_Expected_Time - timedelta(hours=File_Buffer_Time)
    Negative_Buffer_Time = Negative_Buffer_Time.strftime("%H:%M:%S")
    # print(Negative_Buffer_Time)

    if (Source_FileNameValue == Target_FileNameValue) and Negative_Buffer_Time <= File_Arrival_Time <= File_Expected_Time_tt:
        RuleStatus = "On Time"
    else:
        RuleStatus = "Delay"
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": File_Arrival_Time,
                                    "Target_Value": f"{Negative_Buffer_Time} between {File_Expected_Time_tt}",
                                    "Source_Value_Type": 'Source File Arrival Time',
                                    "Target_Value_Type": 'Target File Arrival Time',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    if (RuleStatus == 'On Time' or RuleStatus == 'Delay'):
        out = 'FileArrival_Validation passed'
    else:
        out = 'FileArrival_Validation Failed'

    return out


def CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName,
                                                            BalanceAmountColumn,server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(9)
    given_name =ObjectName.split(".")
    given_format = given_name[-1]
    
    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    SourceValue = int(AuditTableFileCount_pdf['File_Balance_Amount'])

    FileMetaData_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select FileName,BalanceAmountColumn from metaabc.File_MetaData") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    FileMetaDataFilter_df = FileMetaData_df.filter(FileMetaData_df.FileName == ObjectName)
    BalanceAmountColumn1 = FileMetaDataFilter_df.collect()[0][1]

    spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                   dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                   )
    #TargetData_df = spark.read.option("header", "true").parquet(ObjectPath_PostProcessing)
    
    TargetData_df = spark.read.load(path=ObjectPath + ObjectName, format=f"{given_format}", header = True)
    
    BalanceAmount = TargetData_df.agg({BalanceAmountColumn: 'sum'})
    for col in BalanceAmount.dtypes:
        TargetValue = int(BalanceAmount.first()[col[0]])

    if (SourceValue == TargetValue):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Source Balance Amount',
                                    "Target_Value_Type": 'Target Balance Amount',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    # display(Job_Rule_Execution_Log_df)

    if (RuleStatus == 'Passed'):
        out = 'BalanceAmountValidation passed'
    else:
        out = 'BalanceAmountValidation Failed'

    return out


def CheckJobRule_CountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName, server_name,database_name):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(8)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from abc.Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    Src_Count = int(AuditTableFileCount_pdf['File_Count'])

    print(Src_Count)

    spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                   dbutils.secrets.get("adls2adlsscope", "storageaccountkey"))

    # spark.conf.set("fs.azure.account.key.adlstoadls.dfs.core.windows.net",dbutils.secrets.get("nasdrivescope1","testkey"))
    # spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",dbutils.secrets.get("nasdrivescope1","testkey"))
    RawLogData_df = spark.read.parquet(ObjectPath_PostProcessing)
    Tgt_Count = int(RawLogData_df.count())

    # print(Tgt_Count)

    if (Src_Count == Tgt_Count):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": Src_Count,
                                    "Target_Value": Tgt_Count,
                                    "Source_Value_Type": 'Source Count',
                                    "Target_Value_Type": 'Target Count',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": Username}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "abc.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    # display(Job_Rule_Execution_Log_df)

    if (RuleStatus == 'Passed'):
        out = 'CountValidation Post File Processing passed'
    else:
        out = 'CountValidation Post File Processingn Failed'

    return out
