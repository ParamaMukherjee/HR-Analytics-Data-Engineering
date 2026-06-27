# Databricks notebook source
gold_emp_df = spark.table('workspace.default.emp_sil_data')
gold_emp_stat_df = spark.table('workspace.default.emp_stat_sil_data')
gold_comp_per_df = spark.table('workspace.default.comp_per_sil_data')


# COMMAND ----------

#Employee 360 view
from pyspark.sql.functions import *
gold_emp_360 = gold_emp_df.alias("e") \
    .join(
        gold_emp_stat_df.alias("s"),
        col("e.Emp_ID") == col("s.EmpID"),
        "inner"
    ) \
    .join(
        gold_comp_per_df.alias("c"),
        col("e.Emp_ID") == col("c.EmpID"),
        "inner"
    ) \
    .select(
        col("e.Emp_ID"),
        col("e.Employee_Name"),
        col("e.Department"),
        col("e.Position"),
        col("e.Manager_Name"),
        col("e.Date_of_Hire"),
        col("s.Date_of_Termination"),
        col("s.Term_Reason"),
        col("c.Salary"),
        col("c.Performance_Score"),
        col("c.Perf_Score_ID"),
        col("c.Engagement_Survey"),
        col("c.Emp_Satisfaction"),
        col("c.Absences")
    )

# COMMAND ----------

#Department Salary Metrics
from pyspark.sql.functions import *
emp_com_df = gold_emp_df.join(gold_comp_per_df,gold_emp_df.Emp_ID == gold_comp_per_df.EmpID,"inner")

gold_department_salary_metrics = emp_com_df.groupBy('Department').agg(max('Salary').alias('Max_Salary'),min('Salary').alias('Min_Salary'),mean('Salary').cast('decimal(10,2)').alias('Av_Salary')).orderBy(col('Max_Salary').desc())
display(gold_department_salary_metrics)

gold_department_salary_metrics.write.mode('overwrite').saveAsTable('gold_department_salary_metrics')

# COMMAND ----------

#Top Performers by performence rank and salary
from pyspark.sql.window import Window

window_func = Window.partitionBy('Department').orderBy(col('Perf_Score_ID').desc(), col('Salary').desc())
emp_comp_rank = emp_com_df.withColumn('Perf_Rank', row_number().over(window_func))
gold_top_performers = emp_comp_rank.select('Employee_Name','Department','Salary','Performance_Score')

gold_top_performers.write.mode('overwrite').saveAsTable('gold_top_performers')


# COMMAND ----------

# DBTITLE 1,Cell 5
#Department wise Attrition Analysis
emp_dept = gold_emp_df.join(gold_emp_stat_df,gold_emp_df.Emp_ID == gold_emp_stat_df.EmpID,'inner')

total_emp=emp_dept.groupBy('Department').agg(count('*').alias('Total_Employees'))

total_emp_resigned = emp_dept.filter(col('Date_of_Termination').isNotNull()).groupBy('Department').agg(count('*').alias('Total_Employee_Resigned'))

final_emp = total_emp.alias('te').join(total_emp_resigned.alias('er'),col('te.Department') == col('er.Department')).drop(col('er.Department'))

gold_attrition_analysis= final_emp.withColumn('Attrition_Rate',round((col('Total_Employee_Resigned')/col('Total_Employees')*100),2))
display(gold_attrition_analysis.select('te.Department','Total_Employees','Total_Employee_Resigned','Attrition_Rate'))

gold_attrition_analysis.write.mode('overwrite').saveAsTable('gold_attrition_analysis')




# COMMAND ----------

#Employee Tenure

gold_employee_tenure = gold_emp_stat_df.withColumn('Tenure_Years', when(col('Date_of_Termination').isNull(),round(datediff(current_date(),col('Date_of_Hire'))/365,1)).otherwise(round(datediff(col('Date_of_Termination'),col('Date_of_Hire'))/365,1)))
display(gold_employee_tenure)

gold_employee_tenure.write.mode('overwrite').saveAsTable('gold_employee_tenure')


# COMMAND ----------

#Department Productivity

gold_department_productivity=emp_com_df.groupBy('Department').agg(round(avg('Perf_Score_ID'),2).alias('Avg_Performence_Score'),round(avg('Engagement_Survey'),2).alias('Avg_Engagement_Survey'),round(avg('Emp_Satisfaction'),2).alias('Avg_Emp_Satisfaction'),round(avg('Absences'),2).alias('Avg_Absences'))
display(gold_department_productivity)

gold_department_productivity.write.mode('overwrite').saveAsTable('gold_department_productivity')



# COMMAND ----------

#High Salary Low Performance Employees

gold_high_cost_low_performance = gold_comp_per_df.filter((col('Salary')>50000) & (col('Perf_Score_ID')<=2))
gold_high_cost_low_performance=gold_high_cost_low_performance.select('EmpID','Salary','Performance_Score')
display(gold_high_cost_low_performance)

gold_high_cost_low_performance.write.mode('overwrite').saveAsTable('gold_high_cost_low_performance')

# COMMAND ----------

#Special Projects Analysis
gold_special_projects_analysis=gold_comp_per_df.groupBy('Special_Projects_Count').agg(count('EmpID').alias('Employee_Count'),round(avg('Perf_Score_ID'),2).alias('Avg_Perf_Score_ID')).orderBy(col('Special_Projects_Count').desc())
display(gold_special_projects_analysis)

gold_special_projects_analysis.write.mode('overwrite').saveAsTable('gold_special_projects_analysis')


# COMMAND ----------

# DBTITLE 1,Cell 10
#Attendance Risk Report
emp_stat_comp_df = emp_com_df.alias('c').join(gold_emp_stat_df.alias('es'),col('es.EmpID') == col('c.EmpID')).drop(col('es.Date_of_Hire'), col('es.EmpID'))

gold_attendance_risk_report=emp_stat_comp_df.withColumn('Attendance_Risk_Category',when((col('Absences')>10)&(col('Days_Late_Last_30')>=5),'High Risk').when((col('Absences')>5)&(col('Days_Late_Last_30')>=5),'Medium Risk').otherwise('Low Risk'))
display(gold_attendance_risk_report.select('Employee_Name','Department','Absences','Days_Late_Last_30','Performance_Score','Attendance_Risk_Category'))

gold_attendance_risk_report.write.mode('overwrite').saveAsTable('gold_attendance_risk_report')