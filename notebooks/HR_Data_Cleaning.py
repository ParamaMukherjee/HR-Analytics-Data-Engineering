# Databricks notebook source
from pyspark.sql.functions import *
emp_br_df = spark.table('workspace.default.employee_bronze_data')
emp_sil_df= emp_br_df.dropDuplicates()
emp_sil_df = emp_sil_df.withColumn('DOB',when(col('DOB').contains('/'),to_date('DOB','MM/dd/yy')).otherwise(to_date('DOB','MM-dd-yyyy')))
emp_sil_df = emp_sil_df.withColumn('Date_of_Hire',when(col('Date_of_Hire').rlike(r'\d{1,2}[/]\d{1,2}[/]\d{2,4}'),to_date(col('Date_of_Hire'),'M/dd/yyyy')).when(col('Date_of_Hire').rlike(r'/d{1,2}[/]\d{1,2}[/]\d{2,4}'),to_date(col('Date_of_Hire'),'MM/dd/yyyy')).when(col('Date_of_Hire').rlike(r'\d{1,2}[-]\d{1,2}[-]\d{2,4}'),to_date(col('Date_of_Hire'),'MM-dd-yyyy')).otherwise(to_date(col('Date_of_Hire'),'MM-dd-yy')))
emp_sil_df=emp_sil_df.withColumn('Employee_Name',trim('Employee_Name'))
emp_sil_df=emp_sil_df.filter(col('Emp_ID').isNotNull())
display(emp_sil_df)


emp_stat_br_df = spark.table('workspace.default.emp_stat_br_data')
emp_stat_sil_df = emp_stat_br_df.dropDuplicates()
emp_stat_sil_df = emp_stat_sil_df.withColumn('Date_of_Hire',when(col('Date_of_Hire').rlike(r"\d{1,2}[/]\d{1,2}[/]\d{4}"),to_date('Date_of_Hire','M/dd/yyyy')).when(col('Date_of_Hire').rlike(r"\d{1,2}[/]\d{1,2}[/]\d{4}"),to_date('Date_of_Hire','MM/dd/yyyy')).when(col('Date_of_Hire').rlike(r"\d{1,2}[-]\d{1,2}[-]\d{2,4}"),to_date('Date_of_Hire','MM-dd-yyyy')).otherwise(to_date('Date_of_Hire','MM-dd-yy')))

emp_stat_sil_df = emp_stat_sil_df.withColumn('Date_of_Termination',when(col('Date_of_Termination').rlike(r"\d{1,2}[/]\d{1,2}[/]\d{4}"),to_date('Date_of_Termination','M/dd/yyyy')).when(col('Date_of_Termination').rlike(r"\d{1,2}[/]\d{1,2}[/]\d{4}"),to_date('Date_of_Termination','MM/dd/yyyy')).when(col('Date_of_Termination').rlike(r"\d{1,2}[-]\d{1,2}[-]\d{2,4}"),to_date('Date_of_Termination','MM-dd-yyyy')).otherwise(to_date('Date_of_Termination','MM-dd-yy')))

emp_stat_sil_df = emp_stat_sil_df.filter((col('Date_of_Hire')<=col('Date_of_Termination')) | col('Date_of_Termination').isNull())

comp_per_br_df = spark.table('workspace.default.comp_per_br_data')
comp_per_sil_df = comp_per_br_df.dropDuplicates()
comp_per_sil_df = comp_per_sil_df.withColumn('Salary',when(col('Salary')<0,None).otherwise(col('Salary')))



# COMMAND ----------

emp_sil_df.write.mode('overwrite').saveAsTable('emp_sil_data')
emp_stat_sil_df.write.mode('overwrite').saveAsTable('emp_stat_sil_data')
comp_per_sil_df.write.mode('overwrite').saveAsTable('comp_per_sil_data')
