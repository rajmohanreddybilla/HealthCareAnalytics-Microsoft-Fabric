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

# PARAMETERS CELL ********************

source_system = ""
table_name = ""
landing_timestamp = ""
source_file_format = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_landing_path = "abfss://HealthCareAnalytics@onelake.dfs.fabric.microsoft.com/HealthCareAnalyticsLH.Lakehouse/Files"
source_path = f"{base_landing_path}/{source_system}"
table_path = f"{source_path}/{table_name}"
landing_path = f"{table_path}/{landing_timestamp}/"


def path_exists(path: str) -> bool:
    try:
        notebookutils.fs.ls(path)
        return True
    except Exception:
        return False


missing_path = next((path for path in [source_path, table_path, landing_path] if not path_exists(path)), None)

if missing_path:
    message = (
        f"No new files are received for source_system={source_system}, "
        f"table_name={table_name}, landing_timestamp={landing_timestamp}."
    )
    print(message)
    notebookutils.notebook.exit(message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source_file_format == "csv":
    landing_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(landing_path)
else:
    landing_df = spark.read.format("parquet").load(landing_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
updated_df = landing_df.withColumn("landing_timestamp", F.lit(landing_timestamp))\
    .withColumn("insert_timestamp", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df.write.mode("append").saveAsTable(f"bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
