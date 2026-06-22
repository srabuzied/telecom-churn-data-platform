#!/usr/bin/env python
# coding: utf-8

# ### Imports & Start Spark

# In[ ]:


from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.types import NumericType
from pyspark.sql import functions as F      
from pyspark.sql.types import *             
from pyspark.sql.window import Window       
from pathlib import Path                    
import os

# Start a local Spark session 
spark = (
    SparkSession.builder
    .appName("TelecomChurnETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "10")  
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")   # hide INFO spam
print("Spark started!")
spark


# In[3]:


from IPython.display import display, HTML
display(HTML("<style>pre { white-space: pre !important; } </style>"))


# ### Read data & Define Schema

# In[4]:


# EXPLICIT SCHEMAS

CLIENT_SCHEMA = StructType([
    StructField("uniqsubs",         IntegerType(), True),
    StructField("actvsubs",         IntegerType(), True),
    StructField("new_cell",         StringType(),  True),
    StructField("crclscod",         StringType(),  True),
    StructField("asl_flag",         StringType(),  True),
    StructField("totcalls",         IntegerType(), True),
    StructField("totmou",           DoubleType(),  True),
    StructField("totrev",           DoubleType(),  True),
    StructField("adjrev",           DoubleType(),  True),
    StructField("adjmou",           DoubleType(),  True),
    StructField("adjqty",           IntegerType(), True),
    StructField("avgrev",           DoubleType(),  True),
    StructField("avgmou",           DoubleType(),  True),
    StructField("avgqty",           DoubleType(),  True),
    StructField("avg3mou",          IntegerType(),  True),
    StructField("avg3qty",          IntegerType(),  True),
    StructField("avg3rev",          IntegerType(),  True),
    StructField("avg6mou",          DoubleType(),  True),
    StructField("avg6qty",          DoubleType(),  True),
    StructField("avg6rev",          DoubleType(),  True),
    StructField("prizm_social_one", StringType(),  True),
    StructField("area",             StringType(),  True),
    StructField("dualband",         StringType(),  True),
    StructField("refurb_new",       StringType(),  True),
    StructField("hnd_price",        DoubleType(),  True),
    StructField("phones",           DoubleType(), True),
    StructField("models",           DoubleType(), True),
    StructField("hnd_webcap",       StringType(),  True),
    StructField("truck",            DoubleType(), True),
    StructField("rv",               DoubleType(), True),
    StructField("ownrent",          StringType(),  True),
    StructField("lor",              DoubleType(), True),
    StructField("dwlltype",         StringType(),  True),
    StructField("marital",          StringType(),  True),
    StructField("adults",           DoubleType(), True),
    StructField("infobase",         StringType(),  True),
    StructField("income",           DoubleType(), True),
    StructField("numbcars",         DoubleType(), True),
    StructField("HHstatin",         StringType(),  True),
    StructField("dwllsize",         StringType(),  True),
    StructField("forgntvl",         DoubleType(), True),
    StructField("ethnic",           StringType(),  True),
    StructField("kid0_2",           StringType(),  True),
    StructField("kid3_5",           StringType(),  True),
    StructField("kid6_10",          StringType(),  True),
    StructField("kid11_15",         StringType(),  True),
    StructField("kid16_17",         StringType(),  True),
    StructField("creditcd",         StringType(),  True),
    StructField("eqpdays",          DoubleType(), True),
    StructField("Customer_ID",      IntegerType(), False),
])

RECORD_SCHEMA = StructType([
    StructField("rev_Mean",         DoubleType(),  True),
    StructField("mou_Mean",         DoubleType(),  True),
    StructField("totmrc_Mean",      DoubleType(),  True),
    StructField("da_Mean",          DoubleType(),  True),
    StructField("ovrmou_Mean",      DoubleType(),  True),
    StructField("ovrrev_Mean",      DoubleType(),  True),
    StructField("vceovr_Mean",      DoubleType(),  True),
    StructField("datovr_Mean",      DoubleType(),  True),
    StructField("roam_Mean",        DoubleType(),  True),
    StructField("change_mou",       DoubleType(),  True),
    StructField("change_rev",       DoubleType(),  True),
    StructField("drop_vce_Mean",    DoubleType(),  True),
    StructField("drop_dat_Mean",    DoubleType(),  True),
    StructField("blck_vce_Mean",    DoubleType(),  True),
    StructField("blck_dat_Mean",    DoubleType(),  True),
    StructField("unan_vce_Mean",    DoubleType(),  True),
    StructField("unan_dat_Mean",    DoubleType(),  True),
    StructField("plcd_vce_Mean",    DoubleType(),  True),
    StructField("plcd_dat_Mean",    DoubleType(),  True),
    StructField("recv_vce_Mean",    DoubleType(),  True),
    StructField("recv_sms_Mean",    DoubleType(),  True),
    StructField("comp_vce_Mean",    DoubleType(),  True),
    StructField("comp_dat_Mean",    DoubleType(),  True),
    StructField("custcare_Mean",    DoubleType(),  True),
    StructField("ccrndmou_Mean",    DoubleType(),  True),
    StructField("cc_mou_Mean",      DoubleType(),  True),
    StructField("inonemin_Mean",    DoubleType(),  True),
    StructField("threeway_Mean",    DoubleType(),  True),
    StructField("mou_cvce_Mean",    DoubleType(),  True),
    StructField("mou_cdat_Mean",    DoubleType(),  True),
    StructField("mou_rvce_Mean",    DoubleType(),  True),
    StructField("owylis_vce_Mean",  DoubleType(),  True),
    StructField("mouowylisv_Mean",  DoubleType(),  True),
    StructField("iwylis_vce_Mean",  DoubleType(),  True),
    StructField("mouiwylisv_Mean",  DoubleType(),  True),
    StructField("peak_vce_Mean",    DoubleType(),  True),
    StructField("peak_dat_Mean",    DoubleType(),  True),
    StructField("mou_peav_Mean",    DoubleType(),  True),
    StructField("mou_pead_Mean",    DoubleType(),  True),
    StructField("opk_vce_Mean",     DoubleType(),  True),
    StructField("opk_dat_Mean",     DoubleType(),  True),
    StructField("mou_opkv_Mean",    DoubleType(),  True),
    StructField("mou_opkd_Mean",    DoubleType(),  True),
    StructField("drop_blk_Mean",    DoubleType(),  True),
    StructField("attempt_Mean",     DoubleType(),  True),
    StructField("complete_Mean",    DoubleType(),  True),
    StructField("callfwdv_Mean",    DoubleType(),  True),
    StructField("callwait_Mean",    DoubleType(),  True),
    StructField("churn",            IntegerType(), False),
    StructField("months",           IntegerType(), True),
    StructField("Customer_ID",      IntegerType(), False),
])

print(f"CLIENT_SCHEMA : {len(CLIENT_SCHEMA.fields)} fields")
print(f"RECORD_SCHEMA : {len(RECORD_SCHEMA.fields)} fields")


# In[ ]:


# ── Read client.csv ───────────────────────────────────────────────
client_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(CLIENT_SCHEMA)
    .option("mode", "PERMISSIVE")  
    .csv("./data/input/bronze_Batch_Client_1M.csv")
    #.load("gs://telecom-churn-data/bronze/Batch/Client_1M.csv")
)

print(f"client_df rows : {client_df.count()}")
print(f"client_df cols : {len(client_df.columns)}")


# In[ ]:


# ── Read record.csv ───────────────────────────────────────────────
record_df = (
    spark.read
    .option("header", "true")
    .schema(RECORD_SCHEMA)
    .option("mode", "PERMISSIVE")  
    .csv("./data/input/bronze_Batch_Record_1M.csv") 
)

print(f"record_df rows : {record_df.count()}")
print(f"record_df cols : {len(record_df.columns)}")


# In[ ]:


# Join the two tables on Customer_ID 

joined_df = client_df.join(record_df, on="Customer_ID", how="inner")

# Add pipeline metadata  
joined_df = (
        joined_df
        .withColumn("_pipeline_run_ts", F.from_utc_timestamp(F.current_timestamp(), "Africa/Cairo"))
        .withColumn("_pipeline_version", F.lit("1.0.0-local"))
        .withColumn("_source_system",    F.lit("GCS_CSV"))   # was: GCS_CSV
    )


print(f"   Rows : {joined_df.count()}")
print(f"   Cols : {len(joined_df.columns)}")
joined_df.show(5)


# In[ ]:


# ── select importatnt columns  
key_cols = [
    "Customer_ID", "churn", "months",

    "rev_Mean", "mou_Mean", "totmrc_Mean", "ovrmou_Mean", "ovrrev_Mean", 
    "datovr_Mean", "roam_Mean", "change_mou", "change_rev", 
    "drop_vce_Mean", "drop_dat_Mean", "blck_vce_Mean", "blck_dat_Mean", 
    "unan_dat_Mean", "plcd_vce_Mean", "plcd_dat_Mean", "comp_vce_Mean", 
    "custcare_Mean", "cc_mou_Mean", "inonemin_Mean", "mou_cvce_Mean", 
    "mou_cdat_Mean", "drop_blk_Mean", "attempt_Mean",
    
    "uniqsubs", "actvsubs", "crclscod", "asl_flag", "totrev", 
    "adjrev", "adjmou", "adjqty", "avgrev", 
    "prizm_social_one", "area", "refurb_new", "marital", "adults", 
    "income", "eqpdays",

    "_pipeline_run_ts" ,"_pipeline_version","_source_system"
]


focused_df = joined_df.select(key_cols)

print(f"Columns Count: {len(focused_df.columns)}")


# ## Cleaning Data

# In[14]:


# Rename columns to lowercase snake_case 
# "Customer_ID" → "customer_id", "rev_Mean" → "rev_mean"

import re

def to_snake_case(name):
    """Convert any column name to lowercase snake_case."""
    # Insert underscore before uppercase letters
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    # Replace any non-alphanumeric character with underscore
    s = re.sub(r'[^a-zA-Z0-9]', '_', s)
    # Remove repeated underscores and lowercase everything
    s = re.sub(r'_+', '_', s).strip('_').lower()
    return s

# Apply to all column names
for old_name in focused_df.columns:
    new_name = to_snake_case(old_name)
    if old_name != new_name:
        focused_df = focused_df.withColumnRenamed(old_name, new_name)

focused_df


# In[15]:


# Drop duplicate rows 
before = focused_df.count()
focused_df = focused_df.dropDuplicates()
focused_df = focused_df.dropDuplicates(["customer_id"])

# Drop invalid rows in churn column
focused_df = focused_df.filter(F.col("churn").isin(0, 1))
after = focused_df.count()


print(f"Duplicates removed: {before - after} rows dropped")


# In[16]:


focused_df.summary().show()


# In[17]:


# Data Quality
# Fix negative values that shouldn't be negative by clamping it to 0

cleaned_df = focused_df

cols_to_fix = [f.name for f in cleaned_df.schema.fields 
               if isinstance(f.dataType, (DoubleType, IntegerType, LongType)) 
               and "change" not in f.name.lower() 
               and f.name not in ("customer_id", "churn")]


fixed_cols = []

for col_name in cols_to_fix:
    # check if column has negative values
    has_negative = cleaned_df.filter(F.col(col_name) < 0).limit(1).count() > 0
    
    if has_negative:
        fixed_cols.append(col_name)
        
        cleaned_df = cleaned_df.withColumn(
            col_name,
            F.when(F.col(col_name) < 0, 0.0).otherwise(F.col(col_name))
        )


print(f"Columns where negative values were found and fixed: {fixed_cols}\n")

cleaned_df.summary().show()


# In[18]:


#Null Handling 
#Numeric  → Median
#Categorical  → Mode

num_cols = [f.name for f in cleaned_df.schema.fields
     if isinstance(f.dataType, (DoubleType, FloatType, IntegerType, LongType))
     and f.name not in ("churn", "customer_id", "income", "adults")]

numeric_fill = {}

for col in num_cols:
    if cleaned_df.filter(F.col(col).isNull()).count() > 0:
        med = cleaned_df.approxQuantile(col, [0.5], 0.01)[0]
        numeric_fill[col] = med if med is not None else 0.0

numeric_imputed_df = cleaned_df.fillna(numeric_fill)


# Categorical  → Mode
categ_cols = [f.name for f in numeric_imputed_df.schema.fields 
              if isinstance(f.dataType, StringType) 
              and f.name not in ("prizm_social_one")]

categorical_fill = {}

for col in categ_cols:
    if numeric_imputed_df.filter(F.col(col).isNull() | (F.col(col) == "")).count() > 0:
        counts_df = numeric_imputed_df.filter(F.col(col).isNotNull() & (F.col(col) != "")) \
                                      .groupBy(col).count()
        
        windowSpec = Window.orderBy(F.desc("count"))
        
        mode_row = counts_df.withColumn("row_num", F.row_number().over(windowSpec)) \
                            .filter(F.col("row_num") == 1) \
                            .select(col).first()
        
        categorical_fill[col] = mode_row[0] if mode_row is not None else "Unknown"

# temp
imputed_df = numeric_imputed_df.fillna(categorical_fill)


#Special cases 
imputed_df = imputed_df.fillna({
    "income": -1,  #-1 means unknown
    "adults": 0,  #  means unknown
    "prizm_social_one": "Unknown"    
})

print("CHECKING ALL NULLS AFTER FULL IMPUTATION")
imputed_df.select([F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c) for c in imputed_df.columns]).show(vertical=True)


# In[19]:


# Clean up text columns
# Trim whitespace and uppercase important category columns

for col_name in categ_cols:
    if col_name in imputed_df.columns:
        imputed_df = imputed_df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))

print(f"Category columns trimmed and uppercased")


# In[20]:


# detect Outlier &Capping 
imputed_df.cache()
#columns have no negatives only need (one-side capping) upper
normal_cols = [f.name for f in imputed_df.schema.fields 
               if isinstance(f.dataType, (DoubleType, FloatType, IntegerType, LongType)) 
               and "change" not in f.name.lower()
               and f.name not in ("customer_id")]

#columns have negatives need (two-side capping) lower + upper capping
change_cols = [f.name for f in imputed_df.schema.fields 
               if isinstance(f.dataType, (DoubleType, FloatType, IntegerType, LongType)) 
               and "change" in f.name.lower()]


cap_cols = []

total_count = imputed_df.count()


for c in normal_cols + change_cols:
    quantiles = imputed_df.approxQuantile(c, [0.25, 0.75], 0.01)
    
    if len(quantiles) == 2:
        q1, q3 = quantiles
        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outliers_count = imputed_df.filter(
            (F.col(c) < lower) | (F.col(c) > upper)
        ).count()

        ratio = outliers_count / total_count

        if ratio > 0.01:
            cap_cols.append(c)

print(f"Columns selected for capping: {cap_cols}\n")



# CAPPING
capped_df = imputed_df

for col in cap_cols:
    
    if "change" in col.lower():
        # 2-side capping
        quantiles_result = imputed_df.approxQuantile(col, [0.01, 0.99], 0.001)
        
        if len(quantiles_result) == 2:
            q01, q99 = quantiles_result
            
            capped_df = capped_df.withColumn(
                col,
                F.when(F.col(col) < q01, q01)
                 .when(F.col(col) > q99, q99)
                 .otherwise(F.col(col))
            )
    
    else:
        # upper capping
        quantiles_result = imputed_df.approxQuantile(col, [0.99], 0.001)
        
        if quantiles_result:
            q99 = quantiles_result[0]
            
            capped_df = capped_df.withColumn(
                col,
                F.when(F.col(col) > q99, q99)
                 .otherwise(F.col(col))
            )


print("Capping completed successfully!")
capped_df.summary().show()
imputed_df.unpersist()


# In[21]:


len(capped_df.columns)


# ### Feature Engineering: Create New Columns

# In[28]:


# Value Mapping

fe_df = capped_df.withColumn(
    "asl_flag",
    F.when(F.col("asl_flag") == "Y", "Yes")
     .when(F.col("asl_flag") == "N", "No")
)
fe_df.cache()

fe_df = fe_df.withColumn(
    "refurb_new",
    F.when(F.col("refurb_new") == "N", "New")
     .when(F.col("refurb_new") == "R", "Refurbished")
)


fe_df = fe_df.withColumn(
    "prizm_social_mapped",  #New Column
    F.when(F.col("prizm_social_one") == "U", "Urban")
     .when(F.col("prizm_social_one") == "S", "Suburban")
     .when(F.col("prizm_social_one") == "T", "Town")
     .when(F.col("prizm_social_one") == "R", "Rural")
     .when(F.col("prizm_social_one") == "C", "City")
     .otherwise("Unknown")
)


# Binning
# Binning credit categories based on the first letter of the code
fe_df = fe_df.withColumn(
    "crclscod_bin",  #New Column
    F.when(F.col("crclscod").startswith("A"), "Class A")  # Top Tier
     .when(F.col("crclscod").startswith("B"), "Class B")  # High Tier
     .when(F.col("crclscod").startswith("C"), "Class C")  # Mid-High
     .when(F.col("crclscod").startswith("D"), "Class D")  # Mid-Low
     .when(F.col("crclscod").startswith("E"), "Class E")  # Low Tier
     .when(F.col("crclscod").startswith("Z"), "Class Z")  # High Risk
     .otherwise("Class Other") 
)



# In[37]:


# Feature Engineering

# quantile-based binning funvtion
def add_quantile_bins(df, col_name, new_col):   #quantile-based binning
    q1, q2, q3 = df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.01)

    return df.withColumn(
        new_col,
        F.when(F.col(col_name) < q1, "Low")
         .when(F.col(col_name) < q2, "Mid-Low")
         .when(F.col(col_name) < q3, "Mid-High")
         .otherwise("High")
    )

# --------------Revenue Features ---------------

fe_df = fe_df.withColumn(
    "refunds",
    F.round(F.col("totrev") - F.col("adjrev"), 2)
)

#ovrrev_mean 
fe_df = fe_df.withColumn(
    "overage_flag",
    F.when(F.col("ovrrev_mean") > 0, "Yes").otherwise("No")
)

fe_df = fe_df.withColumn(
    "overage_rate", F.round(
    F.when(F.col("totmrc_mean") > 0, F.col("ovrrev_mean") / F.col("totmrc_mean"))
     .otherwise(0.0),2))

fe_df = fe_df.withColumn(
    "overage_status",
    F.when(F.col("overage_rate") <= 0.01, "No Overage")
     .when(F.col("overage_rate") <= 0.10, "Light Overage")  # <10%
     .when(F.col("overage_rate") <= 0.30, "Medium Overage") # 10-30% 
     .otherwise("Heavy Overage"))  # >30%


fe_df = fe_df.withColumn(
    "is_roamer",
    F.when(F.col("roam_mean") > 0, "Yes").otherwise("No"))



fe_df = fe_df.withColumn(
    "customer_status",
    F.when(F.col("change_rev") <= -20, "Significant Drop") 
     .when((F.col("change_rev") > -20) & (F.col("change_rev") < -5), "Slight Drop")
     .when((F.col("change_rev") >= -5) & (F.col("change_rev") <= 5), "Stable")
     .when((F.col("change_rev") > 5) & (F.col("change_rev") <= 20), "Slight Increase")
     .otherwise("Significant Increase") 
)

#--------------customer Features ---------------
fe_df = fe_df.withColumn(
    "customer_loyalty",
    F.when(F.col("months") <= 6, "newcomer")
     .when(F.col("months") <= 12, "mid_term") # 6-12m
     .when(F.col("months") <= 24, "loyal")   # 1-2y
     .otherwise("veteran")  # 2y+
)

fe_df = add_quantile_bins(fe_df, col_name="avgrev", new_col="clv_segment") #Customer Lifetime Value

fe_df = fe_df.withColumn(
    "household_segment",
    F.when((F.col("adults") == 0), "Unknown")
     .when(F.col("adults") == 1, "Single")
     .when(F.col("adults") < 4, "Small Family")
     .otherwise("Big Family")
)

fe_df = fe_df.withColumn(
    "device_age",
    F.when(F.col("eqpdays") <= 365, "New")
     .when(F.col("eqpdays") <= 730, "Mid-Age")
     .otherwise("Old") )


#-------------------Quality---------------
fe_df = fe_df.withColumn(
    "completed_call_rate",
    F.round(
        F.when(
            F.col("plcd_vce_mean") > 0,
            F.when(
                F.col("comp_vce_mean") > F.col("plcd_vce_mean"),
                1.0
            ).otherwise(
                F.col("comp_vce_mean") / F.col("plcd_vce_mean"))
        ).otherwise(0.0),
        2)
)


fe_df = fe_df.withColumn(
    "blocked_call_rate",
    F.round(
    F.when(F.col("plcd_vce_mean") > 0,
    F.when(F.col("blck_vce_mean") > F.col("plcd_vce_mean"), 1.0)
    .otherwise(F.col("blck_vce_mean") / F.col("plcd_vce_mean"))
          ).otherwise(0.0),2)
)


fe_df = fe_df.withColumn(
    "drop_block_call_rate",
    F.round(
        F.when(F.col("attempt_mean") > 0, 
               F.when(F.col("drop_blk_mean") > F.col("attempt_mean"), 1.0)
                .otherwise(F.col("drop_blk_mean") / F.col("attempt_mean"))
        ).otherwise(0.0), 2))


fe_df = fe_df.withColumn(
    "network_quality",
    F.when(F.col("drop_block_call_rate") <= 0.02, "Excellent")   
    .when(F.col("drop_block_call_rate") <= 0.05, "Good")        
    .when(F.col("drop_block_call_rate") <= 0.07, "Poor")        
    .otherwise("Very Poor")                                         
)


fe_df = fe_df.withColumn(
    "data_failure_rate", 
    F.round(
        F.when(
            F.col("plcd_dat_mean") > 0,
            F.when(
                F.col("unan_dat_mean") > F.col("plcd_dat_mean"),
                1.0
            ).otherwise(
                F.col("unan_dat_mean") / F.col("plcd_dat_mean")
            )
        ).otherwise(0.0),
        2)
)


fe_df = fe_df.withColumn(
    "data_experience",
    F.when(F.col("plcd_dat_mean") == 0, "No Data Usage")
     .when(F.col("data_failure_rate") <= 0.02, "Excellent")
     .when(F.col("data_failure_rate") <= 0.05, "Good")
     .when(F.col("data_failure_rate") <= 0.07, "Poor")
     .otherwise("Very Poor")
)

fe_df.show()


# In[38]:


fe_df.summary().show()


# In[39]:


fe_df.coalesce(1).write.csv(f"{BASE}/silver", header=True, mode="overwrite")


# In[ ]:


spark.stop()

