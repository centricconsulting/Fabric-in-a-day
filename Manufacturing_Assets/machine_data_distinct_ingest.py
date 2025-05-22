#!/usr/bin/env python
# coding: utf-8

# ## machine_data_distinct_ingest
# 
# New notebook

# In[ ]:


import pandas as pd

url = "https://raw.githubusercontent.com/centricconsulting/Fabric-in-a-day/main/Manufacturing_Assets/MachineDataDistinct.csv"
pdf = pd.read_csv(url)

df = spark.createDataFrame(pdf)

# 'Files/' maps to the default Lakehouse's file storage
df.write.mode("overwrite").parquet("Files/MachineDataDistinct/")


# In[ ]:


df_machine_data = spark.read.format("parquet").option("header","true").load("abfss://Centric_Fabric_Workshop@onelake.dfs.fabric.microsoft.com/LitmusOEELH.Lakehouse/Files/MachineDataDistinct")

df_machine_data_deduped = df_machine_data.dropDuplicates()
df_machine_data_deduped.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("dimassetitem")


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select * from dimassetitem

