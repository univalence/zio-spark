package zio.spark

import org.apache.spark.sql.{Encoder, Row}
import zio.internal.stacktracer.SourceLocation
import zio.{RIO, Trace}
import zio.spark.test.internal.GenDataMatcher._
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.test.{ErrorMessage, TestArrow, TestResult, TestTrace}

import scala.reflect.ClassTag

package object test {
  val defaultSparkSession: SparkSession.Builder =
    SparkSession.builder
      .master(localAllNodes)
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.ui.enabled", value = false)

  def Dataset[T: Encoder](values: T*)(implicit trace: Trace): SIO[Dataset[T]] = values.toDataset

  def RDD[T: ClassTag](values: T*)(implicit trace: Trace): SIO[RDD[T]] = values.toRDD

  implicit class ExpectOpsZIO[R, E, T](getDataset: RIO[R, Dataset[T]]) {
    def expectAll(matchers: DataMatcher[T]*)(implicit trace: Trace, sourceLocation: SourceLocation): RIO[R, TestResult] =
      for {
        dataset <- getDataset
        rows <- dataset.collect
      } yield (
        TestResult (
          TestArrow.make[Any, Boolean](
            _ => {
              val boolean = rows.forall(row => matchers.exists(_.respect(row)))
              TestTrace.boolean(boolean)(ErrorMessage.text("One of the row does not respect any matcher"))
            }
          ).withCode("Placeholder").withLocation(sourceLocation)
        )
      )
  }

  object row {
    def apply[T](t: T): DataMatcher[T] = (other: T) => t == other
    def apply(value: Any, values: Any*): DataMatcher[Row] = new DataMatcher[Row] {
      override def respect(other: Row): Boolean = ???
    }
  }
}
