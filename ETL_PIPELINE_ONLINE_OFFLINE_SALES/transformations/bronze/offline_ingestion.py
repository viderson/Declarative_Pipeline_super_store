from pyspark import pipelines as dp
from pyspark.sql.functions import *
SOURCE_PATH = "s3://databricks-project-bucket-1/data-store/offline"

@dp.table(
    name = 'offline_bronze',
    comment = "streaming_table_for_offline_records",
    table_properties = {
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    }



)
def offline_bronze():
    df =  spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option('cloudFiles.inferSchema', 'true') \
          .option('cloudFiles.schemaEvolutionMode', 'rescue') \
          .option('cloudFiles.maxFilesPerTrigger', 100) \
          .option('header', 'true') \
          .option('quote', '"') \
          .option('escape', '"') \
          .option('mode', 'PERMISSIVE') \
          .option("fileMetadataCache", "true") \
          .load(SOURCE_PATH) 

    mapping = {c: c.replace(' ', '_').replace('-', '_') for c in df.columns}


    df = df.withColumnsRenamed(mapping) \
           .withColumn('file_path', col("_metadata.file_path")) \
           .withColumn('ingestion_datetime', current_timestamp())  
    return df