package zio.spark.sql

import org.apache.spark.sql.{Sniffer213, SparkSession => UnderlyingSparkSession}

import zio.{Task, ZTraceElement}

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  def withActive[T](block: => T)(implicit trace: ZTraceElement): Task[T] =
    Task.attempt(Sniffer213.sparkSessionWithActive(underlyingSparkSession, block))
}
