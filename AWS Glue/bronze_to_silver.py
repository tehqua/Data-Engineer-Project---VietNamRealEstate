import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Đọc dữ liệu từ folder Bronze
bronze_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/bronze/processed_data.parquet"
df_bronze = spark.read.parquet(bronze_path)

# Tạo DataFrame từ folder Silver 
silver_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/silver//current_data.csv"
try:
    df_silver = spark.read.csv(silver_path, header=True, inferSchema=True)
except Exception as e:
    print("Silver data does not exist yet, starting fresh.")
    df_silver = spark.createDataFrame([], df_bronze.schema)

# Xác định khóa chính 
primary_key = "Link"

# Áp dụng SCD Type 1
window_spec = Window.partitionBy(primary_key).orderBy(col("Updated_Day").desc())
df_bronze = df_bronze.withColumn("row_number", row_number().over(window_spec))
df_bronze = df_bronze.filter(col("row_number") == 1).drop("row_number")

# Hợp nhất dữ liệu từ Bronze và Silver
df_merged = df_silver.unionByName(df_bronze, allowMissingColumns=True).dropDuplicates([primary_key])

# Lưu kết quả dưới định dạng CSV vào Silver Zone
destination_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/silver//current_data.csv"
df_merged.write.mode('overwrite').csv(destination_path, header=True)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
