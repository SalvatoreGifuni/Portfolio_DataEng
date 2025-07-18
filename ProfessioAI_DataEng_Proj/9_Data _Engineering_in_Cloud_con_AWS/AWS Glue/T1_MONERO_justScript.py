import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace, when, col, avg, last, to_date, round

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read price data from S3
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3-sg-bucket-raw/MONERO/XMR_EUR Kraken Historical Data.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert DynamicFrames to DataFrames
df = df.toDF()

# Drop unnecessary columns
df = df.drop('Open', 'High', 'Low', 'Vol.', 'Change %')

# Convert 'Date' column to date type
df = df.withColumn('Date', to_date(df['Date'], 'MM/dd/yyyy'))

# Convert 'Price' column to float type
df = df.withColumn('Price', regexp_replace(col('Price'), ',', '').cast('float'))

# Define a 5-day window for calculating the average
windowSpec5 = Window.orderBy("Date").rowsBetween(-5, -1)

# Calculate the 5-day moving average for non-missing values
df = df.withColumn('Price_Avg_5', avg(when(col('Price') != -1, col('Price'))).over(windowSpec5))

# Replace -1 with the calculated 5-day average
df = df.withColumn(
    'Price',
    when(df['Price'] == -1, df['Price_Avg_5']).otherwise(df['Price'])
)

# Round 'Price' values to two decimal places
df = df.withColumn('Price', round(df['Price'], 2))

# Remove the auxiliary column if not needed
df = df.drop('Price_Avg_5')

# Save the cleaned data to Parquet format on S3
df.write.mode('overwrite').parquet("s3://s3-sg-bucket-silver/MONERO")

job.commit()