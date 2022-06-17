# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
            (
                race_year INT,
                team_name STRING,
                driver_id INT,
                driver_name STRING,
                race_id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
            )
            USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_result_updated
            AS
            SELECT races.race_year,
                   constructors.name AS team_name,
                   drivers.driver_id,
                   drivers.name AS driver_name,
                   races.race_id,
                   results.position,
                   results.points,
                   11 - results.position AS calculated_points
              FROM f1_processed.results
              JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
              JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
              JOIN f1_processed.races ON (results.race_id = races.race_id)
              WHERE results.position <= 10
                AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_result_updated upd
# MAGIC ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.position = upd.position,
# MAGIC     tgt.points = upd.points,
# MAGIC     tgt.calculated_points = upd.calculated_points,
# MAGIC     tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
# MAGIC   VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, CURRENT_TIMESTAMP)