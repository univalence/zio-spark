package zio.spark

import org.apache.spark.sql.Row

import zio.{Task, Trace}
import zio.internal.stacktracer.SourceLocation
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test.internal.Matcher._
import zio.spark.test.internal.Matcher.LineMatcher._
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

  implicit class ExpectOps[T](dataset: Dataset[T]) {
    def expectAll(matchers: LineMatcher[T]*)(implicit trace: Trace, sourceLocation: SourceLocation): Task[TestResult] =
      dataset.collect.map { rows =>
        TestResult(
          TestArrow
            .make[Any, Boolean] { _ =>
              val boolean = rows.forall(row => matchers.exists(_.respect(row)))
              TestTrace.boolean(boolean)(ErrorMessage.text("One of the row does not respect any matcher"))
            }
            .withCode("Placeholder")
            .withLocation(sourceLocation)
        )
      }
  }

  val __ = Anything

  object row {
    def apply[T](predicate: T => Boolean): ConditionalMatcher[T] = ConditionalMatcher(predicate)
    def apply[T](value: T): DataMatcher[T]                       = DataMatcher(value)
    def apply(value: Any, values: Any*): RowMatcher              = RowMatcher(Row(value +: values: _*))
  }
}
