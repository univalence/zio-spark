package zio.spark

import org.apache.spark.sql.{ Column, Dataset, Row }

import scala.util.Try
import zio.spark.wrap.{ Impure, ImpureF }
import zio.spark.{ Spark, ZDataFrame, ZRelationalGroupedDataset }
import zio.{ IO, RIO, Task, ZIO }

package object syntax {

  object over9000 {
    implicit class ZDataframeF(rio: Spark[ZDataFrame]) extends ImpureF(rio) {
      override protected def copy(f: Dataset[Row] => Dataset[Row]): ZDataframeF = execute(f)

      def count: Spark[Long] = execute(_.count())

      def filter(condition: String): Spark[ZDataFrame] = execute(_ filter condition)
    }
  }

  implicit class ZIOOps[R, E, A](private val _value: ZIO[R, E, A]) extends AnyVal {

    @inline
    def >>-[B](f: A => B): ZIO[R, E, B] = _value.map(f)
  }

  implicit class AnyOps[A](private val _value: A) extends AnyVal {
    @inline
    def >-[B](f: A => B): B = f(_value)
  }

  implicit class ValueOps[T](private val _value: T) extends AnyVal {
    @inline
    def fail: IO[T, Nothing] = IO.fail(_value)
  }

  @inline
  implicit def toTask[A <: Impure[_]](t: Try[A]): Task[A] = Task.fromTry(t)

  implicit class toTaskOps[A](private val _value: Try[A]) extends AnyVal {
    @inline
    def toTask: Task[A] = Task.fromTry(_value)
  }

  protected abstract class ZRelationalGroupedDatasetOps[R](get: RIO[R, ZRelationalGroupedDataset]) {
    @inline final def exec[X](f: ZRelationalGroupedDataset => X): RIO[R, X]          = get map f
    @inline final def execM[X](f: ZRelationalGroupedDataset => RIO[R, X]): RIO[R, X] = get >>= f

    @inline final def count: RIO[R, ZDataFrame] = exec(_.count)
  }

  implicit final class ZRelationalGroupedDatasetOpsRIO[R](_value: RIO[R, ZRelationalGroupedDataset])
      extends ZRelationalGroupedDatasetOps[R](_value)
  implicit final class ZRelationalGroupedDatasetOpsTry(_value: Try[ZRelationalGroupedDataset])
      extends ZRelationalGroupedDatasetOps[Any](_value)

  protected abstract class ZDataframeOps[R](get: RIO[R, ZDataFrame]) extends ImpureF(get) {

    override protected def copy(f: Dataset[Row] => Dataset[Row]): ZDataframeOps[R] = execute(f)

    @inline final def count: RIO[R, Long]                        = execute(_.count)
    @inline final def filter(column: Column): RIO[R, ZDataFrame] = execute(_.filter(column))
  }

  implicit final class ZDataframeOpsRIO[R](_value: RIO[R, ZDataFrame]) extends ZDataframeOps[R](_value)
  implicit final class ZDataframeOpsTry(_value: Try[ZDataFrame])       extends ZDataframeOps[Any](_value)

}
