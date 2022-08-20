# Databricks notebook source
def stop_al_streams():
    for stream in spark.streams.active:
        print(f"Stopping the stream " + stream.id)
        stream.stop()
        try: stream.awaitTermination()
        except: pass 

# COMMAND ----------

for stream in spark.streams.active:
  print(f"Stopping the stream " + stream.id)
  stream.stop()
  try: stream.awaitTermination()
  except: pass 

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://hub@cck023.blob.core.windows.net",
  mount_point = "/mnt/hub",
  extra_configs = {"fs.azure.account.key.cck023.blob.core.windows.net":"xq2HRkI4i/U4Y9gvR3/T7lE/dNJ/YBcctqW5HatjlwrHoDKMSACSM/b1QMC15f4+luhx1qaDWXtH+AStQmFgNQ=="})

# COMMAND ----------

dbutils.fs.rm(al_incident_json_loc,recurse = True)
dbutils.fs.mkdirs(al_incident_json_loc)
dbutils.fs.cp(al_incident_json_samples, al_incident_json_loc, recurse = True)

dbutils.fs.rm(al_incident_checkpoint_path, True)
dbutils.fs.rm(al_incident_schema_location, True)

# COMMAND ----------

