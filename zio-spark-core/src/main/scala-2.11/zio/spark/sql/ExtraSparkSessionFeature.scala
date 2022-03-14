package zio.spark.sql

import org.apache.spark.sql.{Sniffer211, SparkSession => UnderlyingSparkSession}

import zio.Task

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  lazy val sessionState = Task.attempt(Sniffer211.sessionState(underlyingSparkSession))
}
