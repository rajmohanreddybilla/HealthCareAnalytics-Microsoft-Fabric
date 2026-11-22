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

landing_timestamp = ""
table_name = ""
merge_keys = ""
load_type = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table(f"bronze.{table_name}").filter(f"landing_timestamp='{landing_timestamp}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

target_table = f"silver.{table_name}"
normalized_load_type = (load_type or "FULL").strip().upper()
merge_key_list = [key.strip() for key in merge_keys.split(",") if key.strip()]

if normalized_load_type == "MERGE":
    if not merge_key_list:
        raise ValueError("merge_keys must be provided when load_type is 'MERGE'.")

    missing_merge_keys = [key for key in merge_key_list if key not in bronze_df.columns]
    if missing_merge_keys:
        raise ValueError(f"merge_keys not found in source data: {missing_merge_keys}")

updated_df = (
    bronze_df.withColumn("insert_timestamp", F.current_timestamp())
    .withColumn("update_timestamp", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if normalized_load_type == "FULL":
    updated_df.write.mode("overwrite").saveAsTable(target_table)
elif normalized_load_type == "APPEND":
    updated_df.write.mode("append").saveAsTable(target_table)
elif normalized_load_type == "MERGE":
    target_exists = spark.catalog.tableExists(target_table)

    if not target_exists:
        updated_df.write.mode("overwrite").saveAsTable(target_table)
    else:
        source_view = "silver_merge_source"
        updated_df.createOrReplaceTempView(source_view)

        merge_condition = " AND ".join(
            [f"target.`{key}` = source.`{key}`" for key in merge_key_list]
        )
        update_assignments = ", ".join(
            [
                f"target.`{column}` = source.`{column}`"
                for column in updated_df.columns
                if column != "insert_timestamp"
            ]
        )
        insert_columns = ", ".join([f"`{column}`" for column in updated_df.columns])
        insert_values = ", ".join([f"source.`{column}`" for column in updated_df.columns])

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING {source_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)
else:
    raise ValueError("load_type must be one of: FULL, APPEND, MERGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from silver.hospitals

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

record_count = updated_df.count()
notebookutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
