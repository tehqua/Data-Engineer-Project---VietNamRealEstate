import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import count, when, col, split, udf
from pyspark.sql.types import FloatType
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Đọc CSV từ S3
data_path = "ss3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/raw/lmq0411_2024-10-12.csv"
df_csv = spark.read.csv(data_path, header=True, inferSchema=True)

# Làm phẳng dữ liệu
df = df_csv.select(
    col("Type").alias("Type"),
    col("Price").alias("Price_million_VND"),
    col("Area").alias("Area_m2"),
    col("Place").alias("Place"),
    col("Link").alias("Link"),
    col("Updated Day").alias("Updated_Day")
)

# Kiểm tra số lượng giá trị null
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Loại bỏ hàng có giá trị null
df = df.dropna()

# Loại bỏ các hàng trùng lặp
df = df.dropDuplicates()

# Chuẩn hóa cột Place và Type
def case_normalization(ne):
    return ' '.join([x.capitalize() for x in ne.split()])

normalize_place_udf = udf(case_normalization)

df = df.withColumn("Place", normalize_place_udf(df["Place"]))
df = df.withColumn("Type", normalize_place_udf(df["Type"]))

# Chuẩn hóa giá trị Price
def normalize_price(price):
    if "thỏa thuận" in price:
        return None
    price = price.replace(',', '.')
    if "tỷ" in price:
        return float(price.replace('tỷ', '').strip()) * 1_000
    elif "triệu" in price:
        return float(price.replace('triệu', '').strip())
    return float(price)

normalize_price_udf = udf(normalize_price, FloatType())
df = df.withColumn("Price_million_VND", normalize_price_udf(df["Price_million_VND"]))

# Tách cột Place thành District và City
df = df.withColumn("District", split(df["Place"], ",")[0]) \
       .withColumn("City", split(df["Place"], ",")[1])

# Xoá cột Place
df = df.drop("Place")

# Chuẩn hóa giá trị Area
def normalize_area(area):
    if area is None or area.strip() == "":
        return None
    area = area.replace(',', '.').replace(' m²', '').strip()
    try:
        return float(area)
    except ValueError:
        return None

normalize_area_udf = udf(normalize_area, FloatType())
df = df.withColumn("Area_m2", normalize_area_udf(df["Area_m2"]))

# Thêm cột price_per_m2
df = df.withColumn("price_per_m2", round(col("Price_million_VND") / col("Area_m2"), 2))

# Lưu kết quả dưới định dạng Parquet trong S3
destination_path = "s3://ai4e-ap-southeast-1-dev-s3-data-landing/lmq0411/bronze/processed_data.parquet"
df.write.mode('overwrite').parquet(destination_path)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
