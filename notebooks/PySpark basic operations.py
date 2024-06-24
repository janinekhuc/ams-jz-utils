# Databricks notebook source
# MAGIC %md
# MAGIC # Some introductory PySpark functionalities.
# MAGIC
# MAGIC These are just some basic fucntionalities iwth some explanations added to them. Some resources
# MAGIC - Databricks has and introduction to PySpark [here](https://docs.databricks.com/en/pyspark/basics.html#rename-columns)
# MAGIC - The extensive PySpark documentation can be found [here](https://spark.apache.org/docs/latest/api/python/index.html)
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F

# infering schema is required as pyspark needs a prespecified schema
# df = spark.read.csv(f"{path_to_source_table}.{table_name}", header=True, inferSchema=True)

# let's pretend we have a table already we can use the sample table 
# first we create a table given data in databricks' datasets
# spark.sql("""CREATE TABLE default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')""")

# we read the table
df = spark.sql('SELECT * FROM default.people10m')


# COMMAND ----------


# showing the dataframe
df.display()

# COMMAND ----------

# row count of dataframe
df.count()

# COMMAND ----------

# similiar to pandas tables, every column in a table requires a schema on databricks
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Column operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select columns

# COMMAND ----------

# selecting columns
df.select(F.col('ssn')).display(5)

# COMMAND ----------

# selecting based on list of columns
cols_to_select = ['firstName', 'lastName']
df.select(*cols_to_select).display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new column

# COMMAND ----------

# concatenating strings into new column
df = df.withColumn("fullName", F.concat(df["firstName"], df["middleName"], df["lastName"]))
df.display()

# COMMAND ----------

# boolean flag
df = df.withColumn("quite_rich_flag", F.col("salary") > 85000)
df.display()

# COMMAND ----------

# if else statement python's equivalent to np.where()
df = (df.withColumn(
    "social_class",
    F.when(df["salary"] < 40000, 'lower')
    .when((df["salary"] > 40000) & (df["salary"] < 80000), 'middle')
    .otherwise('upper'))
      )
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop columns

# COMMAND ----------

# drop columns
df = df.drop("quite_rich_flag")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming column names
# MAGIC

# COMMAND ----------

df.withColumnRenamed("birthDate", "birth_date").display()

# COMMAND ----------

df.select(F.col('birthDate').alias('birth_date')).display()


# COMMAND ----------

# renaming given a dictionary of columns
rename_dict = {
  'birthDate':'birth_date',
  'firstName':'first_name',
  'middleName':'middle_name'
}

df.select([F.col(c).alias(rename_dict.get(c, c)) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Row operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting of rows

# COMMAND ----------

df.orderBy(["id"], ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter rows

# COMMAND ----------

df.filter(F.col("salary") > 85000).display()

# COMMAND ----------

# Filtering with Or condition
df.filter(F.col("firstName").startswith('C') | F.col("firstName").startswith('J') ).display()

# COMMAND ----------

# Filtering with and condition
df.filter(F.col("firstName").startswith('C') & F.col("firstName").startswith('J') ).display()

# COMMAND ----------

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop duplicates

# COMMAND ----------

#drop duplicates (in our case we don't have any duplicates)
df.distinct().count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving data

# COMMAND ----------

# saving the dataframe

(df
 .write
 .format("delta")
 .mode("overwrite") # append is also an option
 .saveAsTable(f"{catalogue}.{schema}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note

# COMMAND ----------

# all of the above could also have been done chained e.g.

(df
 .withColumn("fullName", F.concat(df["firstName"], df["middleName"], df["lastName"]))
 .withColumn("quite_rich_flag", F.col("salary") > 85000)
 .withColumn("social_class",
             F.when(df["salary"] < 40000, 'lower')
             .when((df["salary"] > 40000) & (df["salary"] < 80000), 'middle')
             .otherwise('upper'))
 .drop("quite_rich_flag")
 .select([F.col(c).alias(rename_dict.get(c, c)) for c in df.columns])
 )
