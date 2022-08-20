# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cit_schema_al

# COMMAND ----------

al_in_file_format = "json"
al_out_format = "delta"
al_table = 'cit_schema_al'
al_schema_location="/mnt/hub/schema"
al_json_loc ="/mnt/hub/to_process" 
al_checkpoint_path = '/mnt/hub/_checkpoints'

# COMMAND ----------

dbutils.fs.rm(al_checkpoint_path, recurse = True)
dbutils.fs.rm(al_schema_location, recurse = True)

# COMMAND ----------

dbutils.fs.rm(al_json_loc,recurse = True)
dbutils.fs.mkdirs(al_json_loc)

# COMMAND ----------

 #dbutils.fs.ls('/mnt/hub/json_arch')
dbutils.fs.ls('/mnt/hub/to_process')

# COMMAND ----------

# dbutils.fs.mounts()
# dbutils.fs.ls('/mnt/hub')
# dbutils.fs.ls(al_json_loc)
#dbutils.fs.cp('/mnt/hub/json_arch/citation_Record_NoTypeLocations.json', '/mnt/hub/to_process/citation_Record_NoTypeLocations.json')
#dbutils.fs.cp('/mnt/hub/json_arch/citation_Record_NoType.json', '/mnt/hub/to_process/citation_Record_NoType.json')
dbutils.fs.cp('/mnt/hub/json_arch/citation_Record.json', '/mnt/hub/to_process/citation_Record.json')
# dbutils.fs.cp('/mnt/hub/json_arch/citation_Record_NoLoc.json', '/mnt/hub/to_process/citation_Record_NoLoc.json')

# COMMAND ----------

from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import current_timestamp
stream = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", al_in_file_format)\
    .option("cloudFiles.schemaLocation", al_schema_location)\
    .option("cloudFiles.inferColumnTypes", True) \
    .option("multiLine", True) \
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
    .option("overwriteSchema", "true")\
    .load(al_json_loc)\
    .writeStream.format(al_out_format) \
    .option("mergeSchema", "true")\
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
    .option('checkpointLocation', al_checkpoint_path) \
    .trigger(availableNow=True) \
    .table(al_table) 

# COMMAND ----------

#    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\

write:
    #    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
#    .option("mergeSchema", "true")\

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM cit_schema_al
# MAGIC --SELECT body.citationType  FROM cit_schema_al
# MAGIC SELECT body.defendant.person.locations  FROM cit_schema_al

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED  cit_schema_al

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cit_schema_al_body;
# MAGIC CREATE TABLE cit_schema_al_body
# MAGIC AS
# MAGIC SELECT body
# MAGIC     FROM cit_schema_al

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE EXTENDED  cit_schema_al_body
# MAGIC select * from  cit_schema_al_body

# COMMAND ----------

