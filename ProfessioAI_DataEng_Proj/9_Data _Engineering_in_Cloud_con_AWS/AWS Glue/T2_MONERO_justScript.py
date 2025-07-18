import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import avg, col, round
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read cleaned data from the silver bucket
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3-sg-bucket-silver/MONERO"]},
    format="parquet"
).toDF()

# Calculate the 10-day moving average
windowSpec = Window.orderBy("Date").rowsBetween(-9, 0)
df = df.withColumn('Price_MA_10', avg('Price').over(windowSpec))

# Calculate the weekly average price
df = df.groupBy('Date').agg(
    avg('Price_MA_10').alias('Weekly_Price_MA')
)

# Read Google Trends data from the raw bucket
trend_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3-sg-bucket-raw/MONERO/google_trend_monero.csv"]},
    format="csv",
    format_options={"withHeader": True}
).toDF()

# Merge the datasets based on 'Date' and 'Settimana', keeping only the matching dates.
merged_df = df.join(
    trend_df,
    df['Date'] == trend_df['Settimana'],
    how='inner'
)

# Select final columns
merged_df = merged_df.select(
    trend_df['Settimana'].alias('data'),
    round(merged_df['Weekly_Price_MA'], 2).cast(DecimalType(10, 2)).alias('prezzo'),
    trend_df['Monero_interesse'].alias('indice_google_trend').cast('int')
)

# Save the transformed data in Parquet format
merged_df.write.mode("overwrite").parquet("s3://s3-sg-bucket-gold/MONERO")

job.commit()