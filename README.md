# HR Analytics Data Engineering Pipeline

## 📌 Project Overview

This project demonstrates an **end-to-end HR Analytics Data Engineering Pipeline** built using **PySpark**, **SQL**, and **Databricks** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw HR datasets, performs data cleansing and validation, applies business transformations, and generates business-ready analytical tables. The complete ETL process is orchestrated using **Databricks Workflows**.

---

# 🏗️ Project Architecture

![Project Architecture](architecture/HR_Data_Architecture.drawio%20(1).png)

---

# 🚀 Technologies Used

* PySpark
* Databricks
* Databricks Workflows
* Tableau
* Git & GitHub

---

# 📂 Dataset

The project uses three HR datasets.

| Dataset                      | Description                                                              |
| ---------------------------- | ------------------------------------------------------------------------ |
| Employee_Master.csv          | Employee demographic and organizational information                      |
| Employment_Status.csv        | Hiring, termination, attendance, and employment status information       |
| Compensation_Performance.csv | Employee salary, performance, engagement survey, and project information |

---

# 🔄 ETL Pipeline Flow

```
CSV Files
    │
    ▼
Bronze Layer
    │
    ▼
Silver Layer
    │
    ▼
Gold Layer
    │
    ▼
Databricks Workflow
    │
    ▼
Tableau Dashboard
```

---

# 🥉 Bronze Layer

Raw HR CSV files are ingested into Databricks without applying transformations.

**Tables Created**

* employee_bronze
* employment_status_bronze
* compensation_performance_bronze

---

# 🥈 Silver Layer

The Silver layer focuses on data quality and standardization.

### Data Cleaning Performed

* Removed duplicate records
* Standardized multiple date formats
* Trimmed unwanted whitespaces
* Replaced invalid salary values
* Validated hire and termination dates
* Applied business validation rules
* Performed data quality checks

---

# 🥇 Gold Layer

The Gold layer contains business-ready analytical tables.

### Gold Tables

* Employee 360 View
* Department Salary Metrics
* Top Performers by Department
* Employee Tenure Analysis
* Attrition Analysis
* Attendance Risk Report
* Department Productivity Analysis
* Special Projects Analysis

---

# ⚙️ Databricks Workflow

The pipeline is orchestrated using **Databricks Workflows**.

Execution Flow

```
HR_Data_Extraction
        │
        ▼
HR_Data_Cleaning
        │
        ▼
HR_Data_Transformation
```

Each notebook executes only after the successful completion of the previous notebook.

---

# 📊 Dashboard

The Gold layer tables are used to build an HR Analytics dashboard in Tableau.

The dashboard includes insights such as:

* Employee Distribution
* Department Salary Analysis
* Attrition Rate
* Attendance Risk
* Department Productivity
* Top Performers

> Add your Tableau dashboard screenshot here.

Example:

```markdown
![Dashboard](screenshots/tableau_dashboard.png)
```

---

# 📁 Repository Structure

```
HR-Analytics-Data-Engineering
│
├── architecture/
├── datasets/
├── notebooks/
├── screenshots/
└── README.md
```

---

# 💡 Skills Demonstrated

* ETL Pipeline Development
* Medallion Architecture
* PySpark DataFrame API
* SQL
* Data Cleaning
* Data Validation
* Window Functions
* Joins
* Aggregations
* Business KPI Generation
* Databricks Workflows
* Tableau Integration

---

# 🔮 Future Enhancements

* Implement Delta Lake features (MERGE, OPTIMIZE, VACUUM)
* Add Incremental Data Loading
* Integrate Azure Data Lake Storage (ADLS)
* Deploy using Apache Airflow or Azure Data Factory

---

# 👩‍💻 Author

**Parama Mukherjee**

If you found this project useful, feel free to ⭐ the repository.

