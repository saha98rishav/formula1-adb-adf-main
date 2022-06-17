# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.repartition(1).write.mode("overwrite").format("csv").option("header", "true").csv(f"{presentation_csv}/race_results.csv")

# COMMAND ----------

driver_standings_df = spark.read.format("delta").load(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

driver_standings_df.repartition(1).write.mode("overwrite").format("csv").option("header", "true").csv(f"{presentation_csv}/driver_standings.csv")

# COMMAND ----------

constructor_standings_df = spark.read.format("delta").load(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

constructor_standings_df.repartition(1).write.mode("overwrite").format("csv").option("header", "true").csv(f"{presentation_csv}/constructor_standings.csv")

# COMMAND ----------

calculated_race_results = spark.read.format("delta").load(f"{presentation_folder_path}/calculated_race_results")

# COMMAND ----------

calculated_race_results.repartition(1).write.mode("overwrite").format("csv").option("header", "true").csv(f"{presentation_csv}/calculated_race_results.csv")