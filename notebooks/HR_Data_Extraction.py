# Databricks notebook source
emp_df = spark.read.csv('/Workspace/Users/parama.mukherjeefb@gmail.com/Employee_Master.csv',header=True,    inferSchema=True
)
emp_df.write.mode('append').saveAsTable('Employee_Bronze_Data')

emp_stat_df = spark.read.csv('/Volumes/workspace/default/myvolume/HR_Data_Set/Employment_Status.csv',header=True,inferSchema=True)
emp_stat_df.write.mode('append').saveAsTable('Emp_Stat_Br_Data')

comp_per_df = spark.read.csv('/Volumes/workspace/default/myvolume/HR_Data_Set/Compensation_Performance.csv',header=True,inferSchema=True)
comp_per_df.write.mode('append').saveAsTable('Comp_Per_Br_Data')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `workspace`; select * from `default`.`emp_stat_br_data` limit 100;

# COMMAND ----------

emp_stat_df.printSchema()
emp_df.printSchema()
comp_per_df.printSchema()