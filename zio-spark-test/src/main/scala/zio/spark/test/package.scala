package zio.spark

import org.apache.spark.sql.Encoder
import scala3encoders.given
import zio.{Task, Trace, ZIO}
import zio.internal.stacktracer.SourceLocation
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test.ExpectError._
import zio.spark.test.internal.{ColumnDescription, RowMatcher, SchemaMatcher, ToGlobalValueMatcher, ToPositionalValueMatcher}
import zio.spark.test.internal.RowMatcher._
import zio.spark.test.internal.ValueMatcher._
import zio.test.{TestArrow, TestResult, TestTrace}

import scala.collection.mutable
import scala.reflect.ClassTag

package object test extends LowImplicitsPriority{
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

    def expectAll(schema: SchemaMatcher, matchers: PositionalRowMatcher*)(implicit
        trace: Trace,
        sourceLocation: SourceLocation
    ): Task[TestResult] = {
      val maybeIndexMapping = schema.definitionToSchemaIndex(dataset.schema)

      maybeIndexMapping match {
        case Right(indexMapping) => expectAllInternal(indexMapping, matchers: _*)
        case Left(error) =>
          ZIO.succeed {
            TestResult(
              TestArrow
                .make[Any, Boolean](_ => error.toTestTrace)
                .withLocation
                .withCode("Spark Expect System")
            )
          }
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
              val acc: Either[NoMatch[T], Unit] = Right(())

              val result: Either[NoMatch[T], Unit] =
                rows.foldLeft(acc) { case (curr, row) =>
                  val isMatched =
                    availableMatchers.find(_.process(row, dataset.schema, indexMapping)) match {
                      case Some(matcher) if matcher.isUnique =>
                        availableMatchers -= matcher
                        true
                      case Some(_) => true
                      case None    => false
                    }

                  curr match {
                    case Left(error) =>
                      if (isMatched) Left(error)
                      else if (error.values.length >= 10) Left(error.shorten)
                      else Left(error.add(row))
                    case Right(_) =>
                      if (isMatched) Right(())
                      else Left(NoMatch(List(row)))
                  }
                }

              result match {
                case Left(error) => error.toTestTrace
                case Right(_)    => TestTrace.succeed(true)
              }
            }
            .withCode("Spark Expect System")
            .withLocation
        )
      }
    }
  }

  val __ = PositionalValueMatcher.Anything

  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def global[T: ToGlobalValueMatcher](t: T): GlobalValueMatcher =
    implicitly[ToGlobalValueMatcher[T]].apply(t)

  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def column(name: String): ColumnDescription = ColumnDescription(name, None)

  object row {
    def apply(first: GlobalValueMatcher, others: GlobalValueMatcher*): GlobalRowMatcher =
      GlobalRowMatcher(first +: others, true)

    def apply(first: PositionalValueMatcher, others: PositionalValueMatcher*): PositionalRowMatcher =
      PositionalRowMatcher(first +: others, true)
  }

  object schema {
    def apply(first: ColumnDescription, others: ColumnDescription*): SchemaMatcher = SchemaMatcher(first +: others)
  }
}

trait LowImplicitsPriority {
  @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
  implicit def positional[T: ToPositionalValueMatcher](t: T): PositionalValueMatcher =
    implicitly[ToPositionalValueMatcher[T]].apply(t)
}
