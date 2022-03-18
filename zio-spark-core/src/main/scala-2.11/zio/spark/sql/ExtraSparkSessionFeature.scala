package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio.Task

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession)