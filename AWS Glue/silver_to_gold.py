import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Đọc file từ folder Silver
silver_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/silver//current_data.csv"
df_silver = spark.read.csv(silver_path, header=True, inferSchema=True)

# Drop các giá trị null
df_silver_clean = df_silver.dropna()

# Hiển thị một số thông tin về dữ liệu sạch
df_silver_clean.show()

# Lưu dữ liệu sạch vào folder Gold dưới dạng CSV
gold_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/gold/cleaned_data.csv"
df_silver_clean.write.mode('overwrite').csv(gold_path, header=True)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
