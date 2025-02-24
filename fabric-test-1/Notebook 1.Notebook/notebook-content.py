# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f04f470-6809-45ad-8708-c239dbf522b9",
# META       "default_lakehouse_name": "lh_test_1",
# META       "default_lakehouse_workspace_id": "5372b888-a1d4-44e0-afc8-4ab7afd8edc3",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f04f470-6809-45ad-8708-c239dbf522b9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_test_1.dbo.test LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/test.csv")
# df now is a Spark DataFrame containing CSV data from "Files/test.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load("Tables/dbo/test")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
