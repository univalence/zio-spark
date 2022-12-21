package zio.spark

import org.apache.spark.sql.{
  AnalysisException,
  Dataset => UnderlyingDataset,
  Row,
  SparkSession => UnderlyingSparkSession
}

import zio._

package object sql {
  type DataFrame = Dataset[Row]

  type SIO[A]     = ZIO[SparkSession, Throwable, A]
  type SRIO[R, A] = ZIO[R with SparkSession, Throwable, A]

  /** Wrap an effecful spark job into zio-spark. */
  def fromSpark[Out](f: UnderlyingSparkSession => Out)(implicit trace: Trace): SIO[Out] = SparkSession.attempt(f)

  implicit class DatasetConversionOps[T](private val ds: UnderlyingDataset[T]) extends AnyVal {
    @inline def zioSpark: Dataset[T] = Dataset(ds)
  }

  object syntax {

    /**
     * If you want to skip AnalysisException
     * import zio.spark.sql.syntax.throwAnalysisException._
     */

    object throwsAnalysisException {
      @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
      @throws[AnalysisException]
      implicit def forceThrows[T](analysis: TryAnalysis[T]): T = analysis.getOrThrow
    }
  }
}
