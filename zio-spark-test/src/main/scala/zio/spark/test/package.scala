package zio.spark

import zio.{Task, Trace}
import zio.internal.stacktracer.SourceLocation
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test.internal.{RowMatcher, SchemaMatcher}
import zio.spark.test.internal.RowMatcher._
import zio.spark.test.internal.SchemaMatcher.ColumnDescription
import zio.spark.test.internal.ValueMatcher._
import zio.test.{ErrorMessage, TestArrow, TestResult, TestTrace}

import scala.collection.mutable
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
      expectAllInternal(dataset.schema.indices.map(i => i -> i).toMap, matchers: _*)

    def expectAll(schema: SchemaMatcher, matchers: RowMatcher*)(implicit
        trace: Trace,
        sourceLocation: SourceLocation
    ): Task[TestResult] = {
      val maybeIndexMapping = schema.definitionToSchemaIndex(dataset.schema)
      maybeIndexMapping match {
        case Some(indexMapping) => expectAllInternal(indexMapping, matchers: _*)
        case None               => ???
      }
    }

    @SuppressWarnings(Array("scalafix:Disable.ListBuffer", "scalafix:Disable.-="))
    private def expectAllInternal(indexMapping: Map[Int, Int], matchers: RowMatcher*)(implicit
        trace: Trace,
        sourceLocation: SourceLocation
    ): Task[TestResult] = {
      val availableMatchers = mutable.ListBuffer(matchers: _*)

      dataset.collect.map { rows =>
        TestResult(
          TestArrow
            .make[Any, Boolean] { _ =>
              val boolean =
                rows.forall { row =>
                  availableMatchers.find(_.process(row, Some(dataset.schema), indexMapping)) match {
                    case Some(matcher) if matcher.isUnique =>
                      availableMatchers -= matcher
                      true
                    case Some(_) => true
                    case None    => false
                  }
                }

              TestTrace.boolean(boolean)(ErrorMessage.text("One of the row does not respect any matcher"))
            }
            .withCode("Placeholder")
            .withLocation(sourceLocation)
        )
      }
    }
  }

  val __ : PositionalValueMatcher.Anything.type = PositionalValueMatcher.Anything

  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def valueConversion[T](t: T): PositionalValueMatcher.Value[T] = PositionalValueMatcher.Value(t)

  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def predicateConversion[T](predicate: T => Boolean): GlobalValueMatcher.Predicate[T] =
    GlobalValueMatcher.Predicate(predicate)

  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def columnConversion(name: String): ColumnDescription = ColumnDescription(name, None)

  object row {
    def apply(first: PositionalValueMatcher, others: PositionalValueMatcher*): PositionalRowMatcher =
      PositionalRowMatcher(first +: others, true)

    def apply(first: GlobalValueMatcher, others: GlobalValueMatcher*): GlobalRowMatcher =
      GlobalRowMatcher(first +: others, true)
  }

  object schema {
    def apply(first: ColumnDescription, others: ColumnDescription*): SchemaMatcher = SchemaMatcher(first +: others)
  }
}
