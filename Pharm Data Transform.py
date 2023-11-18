# Databricks notebook source
# key variables
appId = dbutils.secrets.get(scope="datalakepharm610_key", key="clientid")
clientSecret = dbutils.secrets.get(scope="datalakepharm610_key", key="appsecretkey")
tenantId = dbutils.secrets.get(scope="datalakepharm610_key", key="directoryid")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": appId,
       "fs.azure.account.oauth2.client.secret": clientSecret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenantId+"/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://pharm@datalakepharm610.dfs.core.windows.net",
mount_point = "/mnt/pharm",
extra_configs = configs)

# COMMAND ----------

# Set all file locations to variables
tx_file_location = "/mnt/pharm/Transactions - Final.csv"
ins_file_location = "/mnt/pharm/Insurance - Sheet1.csv"
med_file_location = "/mnt/pharm/Medication - Sheet1.csv"
pt_file_location = "/mnt/pharm/Patient_2022_1_1 - Sheet1.csv"

file_type = "csv"

# COMMAND ----------

# Define schemas
from pyspark.sql.types import *

tx_schema = StructType([    
    StructField("Transaction_Id", LongType(), True),
    StructField("Transaction_Date", StringType(), True),
    StructField("Patient_Id", StringType(), True),
    StructField("Patient_Name", StringType(), True),
    StructField("Mock_NDC", StringType(), True),
    StructField("Medication_Name", StringType(), True),
    StructField("Qty", FloatType(), True),
    StructField("Price", FloatType(), True),
    StructField("Paid", FloatType(), True),
    StructField("Insurance_Id", StringType(), True),
    StructField("Void", BooleanType(), True)
    ])

ins_schema = StructType([    
    StructField("Id", LongType(), True),
    StructField("Name", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Amount", FloatType(), True)
    ])

med_schema = StructType([    
    StructField("Id", LongType(), True),
    StructField("Name", StringType(), True),
    StructField("Strength", FloatType(), True),
    StructField("Mock_NDC", StringType(), True),
    StructField("PkgSize", IntegerType(), True),
    StructField("PricePerUnit", FloatType(), True)
    ])

pt_schema = StructType([    
    StructField("Id", LongType(), True),
    StructField("Name", StringType(), True),
    StructField("Birthday", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Insurance_Id", LongType(), True)
    ])

# COMMAND ----------

prefix = {
        "tx":[tx_schema, tx_file_location],
        "ins":[ins_schema, ins_file_location],
        "med":[med_schema, med_file_location],
        "pt":[pt_schema,pt_file_location]
        }
dfs = {}
for i in prefix:
    dfs[i] = spark.read \
    .format(file_type) \
    .schema(prefix[i][0]) \
    .option("header", True) \
    .load(prefix[i][1])

# COMMAND ----------

#for i in dfs:
#    dfs[i].show()

# COMMAND ----------

# Fix date columns to Spark accepted date format
from pyspark.sql.functions import *

dfs["tx"] = dfs["tx"].withColumn("Transaction_Date_format",to_date(dfs["tx"].Transaction_Date, format="M/d/yyyy"))
dfs["pt"] = dfs["pt"].withColumn("Birthday_format",to_date(dfs["pt"].Birthday, format="M/d/yyyy"))

# COMMAND ----------

# Drop duplicates on PK Id
dfs["tx"] = dfs["tx"].dropDuplicates()

# COMMAND ----------

# Fill Nulls
dfs["tx"] = dfs["tx"].na.fill({"Paid":0, "Void":"false"})

# COMMAND ----------

df_pt = dfs["pt"]
df_pt = df_pt.withColumn("IsCurrent",lit(1))

# COMMAND ----------

# Create delta update table from df_pt for SCD Type 2
df_pt.write.mode("overwrite").saveAsTable("pt_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Invalidate any obsolete records in staging table to be updated
# MAGIC MERGE INTO pt_staging
# MAGIC USING pt_updates
# MAGIC ON pt_staging.Id = pt_updates.Id
# MAGIC   AND pt_staging.Insurance_Id != pt_updates.Insurance_Id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   pt_staging.IsCurrent = 0
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform a full insert to maintain historical record
# MAGIC INSERT INTO pt_staging
# MAGIC SELECT * FROM pt_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM pt_staging

# COMMAND ----------

df_pt_staging = spark.read.table("pt_staging")

# COMMAND ----------

# write to adls
dfs["tx"].write.option("header",True).parquet("/mnt/pharm/staging_parquet/transactions")
dfs["ins"].write.option("header",True).parquet("/mnt/pharm/staging_parquet/insurance")
dfs["med"].write.option("header",True).parquet("/mnt/pharm/staging_parquet/medications")
df_pt_staging.write.option("header",True).parquet("/mnt/pharm/staging_parquet/patients")
