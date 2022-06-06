package zio.spark.sql

import org.apache.spark.sql.{Sniffer212, SparkSession => UnderlyingSparkSession}

import zio.{Task, Trace, ZIO}

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  def withActive[T](block: => T)(implicit trace: Trace): Task[T] =
    ZIO.attempt(Sniffer212.sparkSessionWithActive(underlyingSparkSession, block))
}
