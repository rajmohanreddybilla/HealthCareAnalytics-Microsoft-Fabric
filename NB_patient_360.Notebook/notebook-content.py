# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7c722bdd-3919-43d8-a2c1-428fdd8e0afb",
# META       "default_lakehouse_name": "HealthCareAnalyticsLH",
# META       "default_lakehouse_workspace_id": "5f14e6e5-c89a-4fed-82ee-bd157cba2456",
# META       "known_lakehouses": [
# META         {
# META           "id": "7c722bdd-3919-43d8-a2c1-428fdd8e0afb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS HealthCareAnalyticsLH.gold;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE HealthCareAnalyticsLH.gold.patient_360 AS
# MAGIC WITH lab_results_agg AS (
# MAGIC   SELECT
# MAGIC     patient_id,
# MAGIC     CAST(COUNT(*) AS INT) AS total_tests,
# MAGIC     CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
# MAGIC     CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count,
# MAGIC     MAX(test_date) AS last_test_date
# MAGIC   FROM HealthCareAnalyticsLH.silver.lab_results
# MAGIC   GROUP BY patient_id
# MAGIC )
# MAGIC SELECT
# MAGIC   p.patient_id,
# MAGIC   p.first_name,
# MAGIC   p.last_name,
# MAGIC   CAST(p.date_of_birth AS DATE) AS date_of_birth,
# MAGIC   CAST(FLOOR(datediff(current_date(), CAST(p.date_of_birth AS DATE)) / 365.25) AS INT) AS age,
# MAGIC   p.gender,
# MAGIC   p.city,
# MAGIC   p.state,
# MAGIC   p.registered_hospital_id AS hospital_id,
# MAGIC   h.hospital_name,
# MAGIC   p.insurance_provider_id,
# MAGIC   ip.provider_name AS insurance_provider_name,
# MAGIC   COALESCE(lra.total_tests, 0) AS total_tests,
# MAGIC   COALESCE(lra.abnormal_test_count, 0) AS abnormal_test_count,
# MAGIC   COALESCE(lra.critical_test_count, 0) AS critical_test_count,
# MAGIC   lra.last_test_date
# MAGIC FROM HealthCareAnalyticsLH.silver.patients AS p
# MAGIC LEFT JOIN HealthCareAnalyticsLH.silver.hospitals AS h
# MAGIC   ON p.registered_hospital_id = h.hospital_id
# MAGIC LEFT JOIN HealthCareAnalyticsLH.silver.insurance_providers AS ip
# MAGIC   ON p.insurance_provider_id = ip.insurance_provider_id
# MAGIC LEFT JOIN lab_results_agg AS lra
# MAGIC   ON p.patient_id = lra.patient_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df = spark.table("HealthCareAnalyticsLH.gold.patient_360")
record_count = updated_df.count()
notebookutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
