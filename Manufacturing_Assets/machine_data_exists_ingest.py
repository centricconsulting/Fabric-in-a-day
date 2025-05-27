import pandas as pd

url = "https://raw.githubusercontent.com/centricconsulting/Fabric-in-a-day/main/Manufacturing_Assets/MachineDataExists.csv"
pdf = pd.read_csv(url)

df = spark.createDataFrame(pdf)

# 'Files/' maps to the default Lakehouse's file storage
df.write.mode("overwrite").parquet("Files/MachineDataExists/")