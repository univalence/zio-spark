package zio.spark.sql

import org.apache.spark.sql.{Sniffer212Plus, SparkSession => UnderlyingSparkSession}

import zio.{Task, Trace, ZIO}

abstract class ExtraSparkSessionFeature(underlyingSparkSession: UnderlyingSparkSession) {
  def withActive[T](block: => T)(implicit trace: Trace): Task[T] =
    ZIO.attempt(Sniffer212Plus.sparkSessionWithActive(underlyingSparkSession, block))
}
