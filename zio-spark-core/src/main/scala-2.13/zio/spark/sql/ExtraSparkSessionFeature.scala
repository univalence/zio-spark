package zio.spark.sql

import org.apache.spark.sql.{Sniffer213, SparkSession => UnderlyingSparkSession}

import zio.{Task, Trace, ZIO}

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  def withActive[T](block: => T)(implicit trace: Trace): Task[T] =
    ZIO.attempt(Sniffer213.sparkSessionWithActive(underlyingSparkSession, block))
}
