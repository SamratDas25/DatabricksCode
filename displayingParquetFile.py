# Databricks notebook source
spark.conf.set(f"fs.azure.account.key.adlsrawtoadlscurated.dfs.core.windows.net",dbutils.secrets.get("databrickscope","adlsrawtoadlscurated"))
df = spark.read.parquet(f"abfss://curated@adlsrawtoadlscurated.dfs.core.windows.net/employees/2022/12/16/12/")
df.write.format("delta").mode("overwrite").saveAsTable("curateddata")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curateddata