package zio.spark

import org.apache.spark.sql.{Dataset => UnderlyingDataset, Row, SparkSession => UnderlyingSparkSession}

import zio._

package object sql {
  type DataFrame = Dataset[Row]

  type SIO[A]     = RIO[SparkSession, A]
  type SRIO[R, A] = RIO[R with SparkSession, A]

  /** Wrap an effecful spark job into zio-spark. */
  def fromSpark[Out](f: UnderlyingSparkSession => Out): SIO[Out] = SparkSession.attempt(f)

  implicit class DatasetConversionOps[T](private val ds: UnderlyingDataset[T]) extends AnyVal {
    @inline def zioSpark: Dataset[T] = Dataset(ds)
  }
}
