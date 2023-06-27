import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import col
from datetime import date, datetime, timedelta 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


today=date.today()-timedelta(days=0)

def process_csv_to_parq_fin(s3_bucket, key, file_path):

  s3_path_rd = 's3://'+ s3_bucket + '/' + file_path
  s3_path_wr = 's3://'+ s3_bucket + '/' + key + '.parquet'

  df = spark.read.csv(s3_path_rd, header = True)
  df.head(5)
  df.printSchema()

  # Check if any column with null values
  columns = df.columns
  total_records = df.count()
  null_counts = df.select([sum(col(col_name).isNull().cast("int")).alias(col_name) for col_name in columns])

  #Dropping the Columns having all rows null


  for col_name in columns:
    count = null_counts.select(col_name).first()[0]
    
    # Dropping the columns wih all rows null
    if count == total_records:
        print(f"Column '{col_name}' has all rows as null.")
        print(f"Dropping column '{col_name}'")
        df = df.drop(col_name)
    elif col_name not in ['FINCODE', 'Year_end', 'TYPE', 'Flag']:
        print('running column :', col_name)
        
        #casting to float
        df = df.withColumn(col_name, col(col_name).cast("float"))


  #df = df.select(*[col(column).cast("float").alias(column) if column in string_columns else col(column) for column in df.columns])
  df = df.withColumn('load_date', today)
  df.printSchema()
  return df



def process_csv_to_parq_quart(s3_bucket, key, file_path):

  s3_path_rd = 's3://'+ s3_bucket + '/' + file_path

  s3_path_wr = 's3://'+ s3_bucket + '/' + key + '.parquet'

  df = spark.read.csv(s3_path_rd, header = True)

  df.head(5)
  df.printSchema()


  # Check if any column with null values
  columns = df.columns
  total_records = df.count()
  null_counts = df.select([sum(col(col_name).isNull().cast("int")).alias(col_name) for col_name in columns])

  #Dropping the Columns having all rows null


  for col_name in columns:
    count = null_counts.select(col_name).first()[0]
    
    # Dropping the columns wih all rows null
    if count == total_records:
        print(f"Column '{col_name}' has all rows as null.")
        print(f"Dropping column '{col_name}'")
        df = df.drop(col_name)
    elif col_name not in ['FINCODE', 'ResulType', 'NoOfmonths', 'flag']:
        print('running column :', col_name)
        
        #Casting to float
        df = df.withColumn(col_name, col(col_name).cast("float"))
 


  #df = df.select(*[col(column).cast("float").alias(column) if column in string_columns else col(column) for column in df.columns])
  df = df.withColumn('load_date', today)
  df.printSchema()
  return df

# Write financial ration data to s3, in parquet format
s3_bucket = 'tekraj-test2'
key_fin = 'liquide/finance_fr/load_date=2023-06-26/'
file_path_fin = 'liquide_raw_data/finance_fr/load_date=2023-06-26/finance_ratio_2023-06-26.csv'
s3_path_wr = 's3://'+ s3_bucket + '/' + key + '.parquet'

df = process_csv_to_parq_fin(s3_bucket, key_fin, file_path_fin)
df = df.withColumn('load_date', today)
df.printSchema()
df.write.mode("overwrite").partitionBy("load_date").parquet(s3_path_wr)  


# write quarterly cons data to s3, in parquet format
key_quart = 'liquide/finance_fr/load_date=2023-06-26/'
file_path_quart = 'liquide_raw_data/quarterly_cons/load_date=2023-06-26/fquarterly_cons_2023-06-26.csv'
s3_path_wr = 's3://'+ s3_bucket + '/' + key_quart + '.parquet'

df = process_csv_to_parq_quart(s3_bucket, key_quart, file_path_quart)
df = df.withColumn('load_date', today)
df.printSchema()
df.write.mode("overwrite").partitionBy("load_date").parquet(s3_path_wr)
