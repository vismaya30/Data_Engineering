from pyspark.sql import SparkSession

from src.common.config import Settings

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_spark(app_name: str, settings: Settings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
    )
    if settings.is_s3:
        builder = (
            builder.config("spark.jars.packages", HADOOP_AWS_PACKAGE)
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.aws_region)
        )
    return builder.getOrCreate()
