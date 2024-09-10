# Databricks notebook source
# MAGIC %md
# MAGIC # SCD2 demo notebook
# MAGIC
# MAGIC Demonstrates the functioning of the scd2 assuming full loads. The notebook shows on three simple "loads" what the scd2 would look like

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

# copy functions in here or append the correct path
from scd2_utils import apply_scd2
import sys
sys.path.append('../src')

# COMMAND ----------

# MAGIC %md
# MAGIC ## First load
# MAGIC Pretending what would happen if the table is initially loaded.

# COMMAND ----------

df_data = [
    {"id": 1, "name": "John Doe", "email": "johnny.doe@example.com"},
    {"id": 3, "name": "Jake Doe", "email": "jake.doe@example.com"},
    {"id": 5, "name": "Jan Doe", "email": "jan.doe@example.com"}
]
# target
df = spark.createDataFrame(df_data)

# COMMAND ----------

# load in recent df, pretending it to be empty here (assuming test_table is the name of your normal table to be saved)
try:
    df_tgt = spark.sql('SELECT * FROM test_table')
except Exception as e:
    df_tgt = spark.createDataFrame([{}])

# COMMAND ----------

# apply scd2
df_scd2 = apply_scd2(df_src=df, df_tgt=df_tgt,
                     id_cols=['id'], hash_data_cols=['name', 'email'],
                     date=None, create_meta_valid_cols=True)

# COMMAND ----------

df_scd2.display()

# COMMAND ----------

df_scd2.write.mode('overwrite').saveAsTable('test_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second Load

# COMMAND ----------

new_data = [
    {"id": 3, "name": "Jake Doe", "email": "jake.doe@example.com"},  # unchanged rows
    {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},  # Changed email
    {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"}  # New record
    # 5 deleted
]
new_df = spark.createDataFrame(new_data)

# COMMAND ----------

# load in recent df, pretending it to be empty here
try:
    df_tgt = spark.sql('SELECT * FROM test_table')
    print('Loaded in test_table')
except Exception as e:
    df_tgt = spark.createDataFrame([{}])

# COMMAND ----------

# apply scd2
df_scd2 = apply_scd2(df_src=new_df, df_tgt=df_tgt,
                     id_cols=['id'], hash_data_cols=['name', 'email'],
                     date=None, create_meta_valid_cols=True)

# COMMAND ----------

df_scd2.display()

# COMMAND ----------

df_scd2.write.mode('overwrite').saveAsTable('test_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third Load

# COMMAND ----------

new_data = [
    {"id": 3, "name": "Jake Doe", "email": "jake.doe@example.com"},  # unchanged
    {"id": 1, "name": "John Does", "email": "john.doe@example.com"},  # Changed name
    {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"}  # unchanged

]
# new load
new_df = spark.createDataFrame(new_data)

# COMMAND ----------

# apply scd2
df_scd2 = apply_scd2(df_src=new_df, df_tgt=df_tgt,
                     id_cols=['id'], hash_data_cols=['name', 'email'],
                     date=None, create_meta_valid_cols=True)

# COMMAND ----------

df_scd2.display()
