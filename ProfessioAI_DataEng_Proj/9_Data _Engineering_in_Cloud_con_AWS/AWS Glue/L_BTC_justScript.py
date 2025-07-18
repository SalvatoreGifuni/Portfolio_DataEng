import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

def convert_types(rec):
    # Convert the 'data' field from string to date
    rec['data'] = datetime.strptime(rec['data'], '%Y-%m-%d').date()
    # Convert the 'prezzo' field to float
    rec['prezzo'] = float(rec['prezzo'])
    # Convert the 'indice_google_trend' field to int
    rec['indice_google_trend'] = int(rec['indice_google_trend'])
    return rec

# Retrieve the job name from the job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Parquet data from S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3-sg-bucket-gold/BTC"]},
    format="parquet"
)

# Apply the transformation to convert data types
converted_datasource = Map.apply(frame=datasource, f=convert_types)

# Apply mapping to ensure correct data types
mapped_datasource = ApplyMapping.apply(
    frame=converted_datasource,
    mappings=[
        ("data", "date", "data", "date"),
        ("prezzo", "double", "prezzo", "double"),
        ("indice_google_trend", "int", "indice_google_trend", "int")
    ]
)

# Write the DynamicFrame to Redshift Serverless using the cataloged connection
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=mapped_datasource,
    catalog_connection="my_redshift_connection",
    connection_options={"dbtable": "my_btc", "database": "dev"},
    redshift_tmp_dir="s3://sg-temporary-dir/BTC/"
)

# Commit the job to mark it as complete
job.commit()