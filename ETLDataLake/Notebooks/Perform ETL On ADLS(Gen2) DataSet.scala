// Databricks notebook source
// DBTITLE 1,Access ADLS Gen2 with a Shared Key
spark.conf.set("fs.azure.account.key.accountsamplestorage.dfs.core.windows.net", "yTQRIG8rQlIys/yabImgJch0M3QHKIkVV4vsRzb/SrJc+ng6vuCZgnTsJcBsxFUo6A0s07EBdJl6lSWrd+vuEQ==")

dbutils.fs.ls("abfss://sample@accountsamplestorage.dfs.core.windows.net/")

// COMMAND ----------

// DBTITLE 1,Read the CSV file from Storage
val df = spark.read.option("header", "true").csv("abfss://sample@accountsamplestorage.dfs.core.windows.net/data/2006.csv")

// COMMAND ----------

// DBTITLE 1,Print Schema of the CSV file
df.printSchema()

// COMMAND ----------

// DBTITLE 1,Count the number of Records
df.count()

// COMMAND ----------

// DBTITLE 1,Save the CSV file records in a SparkSQL Delta Table
df.write.format("delta").saveAsTable("airtraffic")

// COMMAND ----------

// DBTITLE 1,Describe the SaprkSQL Table
// MAGIC %sql
// MAGIC describe extended airtraffic;

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables

// COMMAND ----------

// DBTITLE 1,Display the Records
// MAGIC %sql
// MAGIC select * from airtraffic

// COMMAND ----------

// DBTITLE 1,Insert a new Record in the Table
// MAGIC %sql
// MAGIC INSERT INTO airtraffic VALUES(2010,4,11,3,743,745,1024,1018,"US",343,"N657AW",281,273,223,6,-2,"ATL","PHX",1587,45,13,0,null,0,0,0,0,0,0)

// COMMAND ----------

// DBTITLE 1,Display the Records with a condition
// MAGIC %sql
// MAGIC SELECT * FROM airtraffic where Year = "2010" 

// COMMAND ----------

// DBTITLE 1,Display the History of the Table
// MAGIC %sql
// MAGIC DESCRIBE history default.airtraffic

// COMMAND ----------

// DBTITLE 1,Aggregation on the Data in SQL
// MAGIC %sql
// MAGIC select year,month, count(*) as count from airtraffic group by year,month order by month,year

// COMMAND ----------

val sqlDF = spark.sql("select year,month, count(*) as count from airtraffic group by year,month order by month,year")

// COMMAND ----------

// DBTITLE 1,Save the Aggregated data back to ADLS Gen2 Storage 
sqlDF.write.format("parquet").csv("abfss://sample@accountsamplestorage.dfs.core.windows.net/reports/")

// COMMAND ----------

// DBTITLE 1,Drop the Table in SparkSQL
// MAGIC %sql
// MAGIC drop table airtraffic

// COMMAND ----------


