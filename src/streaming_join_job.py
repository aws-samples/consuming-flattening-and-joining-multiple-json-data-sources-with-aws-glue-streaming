import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import explode, col, expr, from_unixtime, current_timestamp
from pyspark.sql import DataFrame
import datetime


args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'output_path'])

job_name = args['JOB_NAME']
output_path = args['output_path']
checkpoint_location = output_path + "/" + job_name + "/checkpointing"
s3_target = output_path  + "/" + job_name  +  "/dataOutput"

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

logger = glueContext.get_logger()


def process_batch(joined_df, batch_id):

    if joined_df.schema != []:

        joined_gdf = DynamicFrame.fromDF(joined_df, glueContext, "from_data_frame")

        joined_gdf.printSchema()
        joined_df.show(vertical=True, truncate=False)

        now = datetime.datetime.now()
        s3path = s3_target + "/ingest_year=" + "{:0>4}".format(str(now.year)) \
                 + "/ingest_month=" + "{:0>2}".format(str(now.month)) \
                 + "/ingest_day=" + "{:0>2}".format(str(now.day)) \
                 + "/ingest_hour=" + "{:0>2}".format(str(now.hour)) \
                 + "/ingest_minute=" + "{:0>2}".format(str(now.minute)) + "/"

        s3sink = glueContext.write_dynamic_frame.from_options(
            frame=joined_gdf,
            connection_type="s3",
            connection_options={"path": s3path},
            format = "parquet",
            transformation_ctx="s3sink"
        )
        logger.info(f" ========== Batch [{batch_id}] Write Results {s3sink} ==============")


def flatten_nested_purchase(nested_json_df: DataFrame) -> DataFrame:

    raw_output_df = nested_json_df \
        .withColumn("basket_items", explode("body.basket_items")) \
        .select("body.purchase_time", "body.transaction_id", "body.user_id", "basket_items.*")

    output_df = raw_output_df.select("*", "product.*").drop("product").select("*", "pricing.*").drop("pricing")

    return output_df

'''
The JSON schema's is defined in the AWS Glue Data Catalog. Leveraging the below library retrieves the data from 
the Kinesis Stream and applies the schema. See the cloudformation template for a closer look at the syntax required to
define nested JSON schema's.
'''
nested_product_kinesis_df = glueContext.create_data_frame_from_catalog(
    database="awsglue-streaming-join-nested-json-db",
    table_name="nested-purchase-stream-table",
    transformation_ctx="nested_purchase_ds",
    additional_options={"startingPosition": "latest"}
)
recommendations_kinesis_df = glueContext.create_data_frame_from_catalog(
   database="awsglue-streaming-join-nested-json-db",
   table_name="recommender-stream-table",
   transformation_ctx="recommender_ds",
   additional_options={"startingPosition": "latest"}
)


flattened_products_df = flatten_nested_purchase(nested_product_kinesis_df)
flattened_products_ts_df = flattened_products_df\
    .withColumn("purchase_time", from_unixtime(col("purchase_time"))
                .cast("timestamp"))

flattened_products_with_watermark_df = flattened_products_ts_df.\
    withWatermark("purchase_time", "10 minutes")

recommendations_with_watermark_df = recommendations_kinesis_df\
    .withColumn("original_purchase_time", from_unixtime(col("purchase_time")).cast('timestamp'))\
    .withWatermark("original_purchase_time", "30 seconds")\
    .withColumn("recommendation_processed_time", current_timestamp())\
    .drop(col("purchase_time"))


flattened_products_with_recommendation_df = flattened_products_with_watermark_df.join(
  recommendations_with_watermark_df,
  expr("""
        transaction_id = purchase_id AND
        recommendation_processed_time >= purchase_time AND
        recommendation_processed_time <= purchase_time + interval 30 SECONDS
    """), "inner"
)


glueContext.forEachBatch(
    frame=flattened_products_with_recommendation_df,
    batch_function=process_batch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": checkpoint_location
    }
)

job.commit()