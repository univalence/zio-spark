package zio.spark.wrap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import zio._
import zio.spark._
import zio.spark.wrap.Wrap.Aux

import scala.util.Try

sealed trait LowLowPriorityWrap {
  final implicit def _any[T]: Wrap.Aux[T, ZWrap[T]] = new Wrap[T] {
    override type Out = ZWrap[T]
    override def apply(a: T): Out = new ZWrap[T](a) {}
  }
}

sealed trait LowPriorityWrap extends LowLowPriorityWrap {

  final implicit def _dataset[T]: Aux[Dataset[T], ZDataset[T]] = Wrap.zwrap(ds => new ZDataset(ds))
}

abstract class ZWrap[V](private val value: V) {

  /** ...
   * ...
   *
   * @usecase def execute[B](f: V => B):Task[B]
   */
  final def execute[B, C](f: V => B)(implicit W: Wrap.Aux[B, C]): Task[C] = Task(W(f(value)))

  final def executeM[R, B, C](f: V => RIO[R, B])(implicit W: Wrap.Aux[B, C]): RIO[R, C] =
    Task(f(value).map(W.apply)).flatten

  final protected def executeTotal[B, C](f: V => B)(implicit W: Wrap.Aux[B, C]): UIO[C] = UIO(W(f(value)))

  final protected def executeTotalM[R, E, B, C](f: V => ZIO[R, E, B])(implicit W: Wrap.Aux[B, C]): ZIO[R, E, C] =
    f(value).map(W.apply)

  final protected def unsafeTotal[B, C](f: V => B)(implicit W: Wrap.Aux[B, C]): C = W(f(value))

  final protected def unsafe[B, C](f: V => B)(implicit W: Wrap.Aux[B, C]): Try[C] = Try(W(f(value)))

  final protected def executeNoWrap[B](f: V => B): Task[B] = Task(f(value))
}

trait Wrap[A] {
  type Out

  @inline
  def apply(a: A): Out
}

object Wrap extends LowPriorityWrap {
  type Aux[A, B] = Wrap[A] {
    type Out = B
  }

  type NoWrap[T] = Aux[T, T]

  final protected[spark] def noWrap[T]: NoWrap[T] = new Wrap[T] {
    override type Out = T

    @inline
    override def apply(a: T): Out = a
  }

  final protected[spark] def zwrap[A, B <: ZWrap[_]](f: A => B): Aux[A, B] = new Wrap[A] {
    override type Out = B
    override def apply(a: A): Out = f(a)
  }

  implicit val _string: NoWrap[String]            = noWrap
  implicit val _int: NoWrap[Int]                  = noWrap
  implicit val _long: NoWrap[Long]                = noWrap
  implicit val _unit: NoWrap[Unit]                = noWrap
  implicit val _row: NoWrap[Row]                  = noWrap
  implicit val _column: NoWrap[Column]            = noWrap
  implicit def _wrapped[T <: ZWrap[_]]: NoWrap[T] = noWrap

  implicit def _rdd[T]: Aux[RDD[T], ZRDD[T]]                   = zwrap(rdd => new ZRDD(rdd))
  implicit val _dataframe: Aux[DataFrame, ZDataFrame]          = zwrap(df => new ZDataFrame(df))
  implicit val _sparkSession: Aux[SparkSession, ZSparkSession] = zwrap(ss => new ZSparkSession(ss))
  implicit val _sparkContext: Aux[SparkContext, ZSparkContext] = zwrap(sc => new ZSparkContext(sc))

  implicit def _seq[A, B](implicit W: Aux[A, B]): Aux[Seq[A], Seq[B]] = new Wrap[Seq[A]] {
    override type Out = Seq[B]
    override def apply(a: Seq[A]): Out = a.map(W.apply)
  }

  implicit def _option[A, B](implicit W: Aux[A, B]): Aux[Option[A], Option[B]] = new Wrap[Option[A]] {
    override type Out = Option[B]
    override def apply(a: Option[A]): Out = a.map(W.apply)
  }

  def apply[A, B](a: A)(implicit W: Wrap[A]): W.Out = W(a)

}
