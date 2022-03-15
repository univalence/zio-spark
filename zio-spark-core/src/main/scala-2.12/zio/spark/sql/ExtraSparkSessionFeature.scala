package zio.spark.sql

import org.apache.spark.sql.{Sniffer212, SparkSession => UnderlyingSparkSession}

import zio.Task

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  def withActive[T](block: => T): Task[T] =
    Task.attempt(Sniffer212.sparkSessionWithActive(underlyingSparkSession, block))
}
