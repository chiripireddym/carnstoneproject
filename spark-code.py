from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,avg,sum,rank
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Loan Approval").getOrCreate()
schema1 = StructType([
    StructField("C_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("UIN", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("PINCODE", LongType(), True),
    StructField("CibilScore", LongType(), True),
    StructField("DefaulterFlag", StringType(), True)
])  

schema2 = StructType([
    StructField("C_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("UIN", StringType(), True),
    StructField("MAILID", StringType(), True),
    StructField("PHONENUMBER", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("LIVINGSTATUS", StringType(), True),
    StructField("PINCODE", StringType(), True),
    StructField("LOANAMOUNT", LongType(), True)
])

crd = spark.read.csv("/content/sample_data/ClientReferenceDataset.csv", schema = schema1)

hlad = spark.read.csv("/content/sample_data/HomeLoanApplicationData.csv", schema = schema2)
print(hlad.count())
apl_count = hlad.filter(col("LIVINGSTATUS") == "APL").count()
bpl_count = hlad.filter(col("LIVINGSTATUS") == "BPL").count()
print("Total people above Property line", apl_count)
print("Total people below Property line", bpl_count)
defaulter_df = crd.filter(col("DefaulterFlag") == "Y")
#defaulter_df.show()
combined_df = crd.join(hlad, on="C_Name", how="inner")
combined_df = combined_df.drop(hlad["UIN"])
#combined_df.show(5)
approved_df = combined_df.withColumn("LOANSTATUS", when((col("CibilScore")>800) & 
                                                      (col("DefaulterFlag") == "Y"), "APPROVED")
                                                      .otherwise("Rejected")) 
approved_df.show(2) 

details_df = approved_df.select(col("UIN"),col("C_Name"),col("LOANSTATUS"),col("LOANAMOUNT"))   
details_df.write.csv("/content/sample_data/loan_status_details1.csv", header=True)
details_df.orderBy(col("LOANSTATUS").asc()).orderBy(col("LOANAMOUNT").desc()).show()
