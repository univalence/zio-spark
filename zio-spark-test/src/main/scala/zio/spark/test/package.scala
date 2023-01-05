package zio.spark

import org.apache.spark.sql.Row
import zio.{Task, Trace}
import zio.internal.stacktracer.SourceLocation
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test.internal.Matcher.RowMatcher._
import zio.spark.test.internal.Matcher._
import zio.spark.test.internal.ValueMatcher._
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
    def expectAll(matchers: RowMatcher*)(implicit trace: Trace, sourceLocation: SourceLocation): Task[TestResult] =
      dataset.collect.map { rows =>
        TestResult(
          TestArrow
            .make[Any, Boolean] { _ =>
              val boolean = rows.forall(row => matchers.exists(matcher => RowMatcher.process(matcher, row, Some(dataset.schema))))
              TestTrace.boolean(boolean)(ErrorMessage.text("One of the row does not respect any matcher"))
            }
            .withCode("Placeholder")
            .withLocation(sourceLocation)
        )
      }
  }

  val __ = PositionalValueMatcher.Anything
  implicit def valueConversion[T](t: T): PositionalValueMatcher.Value[T] = 
    PositionalValueMatcher.Value(t)
    
  implicit def predicateConversion[T](predicate: T => Boolean): GlobalValueMatcher.Predicate[T] =
    GlobalValueMatcher.Predicate(predicate)

  object row {
    def apply(first: PositionalValueMatcher, others: PositionalValueMatcher*): PositionalRowMatcher = 
      PositionalRowMatcher(first +: others)

    def apply(first: GlobalValueMatcher, others: GlobalValueMatcher*): GlobalRowMatcher =
      GlobalRowMatcher(first +: others)
  }
}
