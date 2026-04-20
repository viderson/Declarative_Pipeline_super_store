
from pyspark import pipelines as dp
from pyspark.sql.functions import *

SOURCE_PATH = "s3://databricks-project-bucket-1/data-store/online"

@dp.table(
    name = "online_bronze",
    comment = "straming_table_for_online_records",
    table_properties = {
        "quality": 'bronze',
        "layer": 'bronze',
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def online_bronze():
                                #włączenie AutoLoadera wylacznie readStream.
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.schemaEvolutionMode","rescue") \
        .option("cloudFiles.maxFilesPerTrigger", 1000) \
        .option("cloudFiles.useNotifications", "false") \
        .option("header", "true") \
        .option('quote', '"') \
        .option('escape', '"') \
        .option("mode", "PERMISSIVE") \
        .option("fileMetadataCache", "true") \
        .load(SOURCE_PATH)

    mapping = {c: c.replace(' ', '_').replace('-', '_') for c in df.columns}

    df = df.withColumnsRenamed(mapping) \
           .withColumn('file_path', col("_metadata.file_path")) \
           .withColumn('ingestion_datetime', current_timestamp())  
    return df