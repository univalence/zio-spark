package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession)