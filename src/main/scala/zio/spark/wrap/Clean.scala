package zio.spark.wrap

import zio._

import zio.spark._
import zio.spark.wrap.Clean.Aux

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.Try

trait Clean[A] {
  type Out

  @inline
  def apply(a: A): Out
}

sealed trait LowPriorityClean1 {
  final implicit def _any[T]: Clean.Aux[T, Impure[T]] = new Clean[T] {
    override type Out = Impure[T]
    override def apply(a: T): Out = new Impure[T](a) {}
  }
}

sealed trait LowPriorityClean0 extends LowPriorityClean1 {
  final implicit def _dataset[T]: Aux[Dataset[T], ZDataset[T]] = Clean.impure(ds => new ZDataset(ds))
}

object Clean extends LowPriorityClean0 {
  type Aux[A, B] = Clean[A] {
    type Out = B
  }

  type Pure[T] = Aux[T, T]

  final protected[spark] def pure[T]: Pure[T] = new Clean[T] {
    override type Out = T

    @inline
    override def apply(a: T): Out = a
  }

  final protected[spark] def impure[A, B <: Impure[_]](f: A => B): Aux[A, B] = new Clean[A] {
    override type Out = B

    override def apply(a: A): Out = f(a)
  }

  implicit val _string: Pure[String]   = pure
  implicit val _int: Pure[Int]         = pure
  implicit val _long: Pure[Long]       = pure
  implicit val _unit: Pure[Unit]       = pure
  implicit val _boolean: Pure[Boolean] = pure
  implicit val _row: Pure[Row]         = pure
  implicit val _column: Pure[Column]   = pure

  implicit def _wrapped[T <: Impure[_]]: Pure[T] = pure

  implicit def _rdd[T]: Aux[RDD[T], ZRDD[T]] = impure(rdd => new ZRDD(rdd))

  implicit val _dataframe: Aux[DataFrame, ZDataFrame]          = impure(df => new ZDataFrame(df))
  implicit val _sparkSession: Aux[SparkSession, ZSparkSession] = impure(ss => new ZSparkSession(ss))
  implicit val _sparkContext: Aux[SparkContext, ZSparkContext] = impure(sc => new ZSparkContext(sc))

  implicit def _seq[A, B](implicit W: Aux[A, B]): Aux[Seq[A], Seq[B]] = new Clean[Seq[A]] {
    override type Out = Seq[B]

    override def apply(a: Seq[A]): Out = a.map(W.apply)
  }

  implicit def _option[A, B](implicit W: Aux[A, B]): Aux[Option[A], Option[B]] = new Clean[Option[A]] {
    override type Out = Option[B]

    override def apply(a: Option[A]): Out = a.map(W.apply)
  }

  implicit val _relationalgroupeddataset: Aux[RelationalGroupedDataset, ZRelationalGroupedDataset] =
    impure(rgd => ZRelationalGroupedDataset(rgd))

  //def apply[A](implicit W: Wrap[A]): W.type = W

  def apply[A](a: A)(implicit W: Clean[A]): W.Out = W(a)

  def effect[A](a: => A)(implicit W: Clean[A]): Task[W.Out] = Task(W(a))
}

abstract class Impure[+V](private val value: V) {

  /**
   * @usecase def execute[B](f: V => B):Task[B]
   */
  final def execute[B, Pure](f: V => B)(implicit W: Clean.Aux[B, Pure]): Task[Pure] = Task(W(f(value)))

  final def executeM[R, B, Pure](f: V => RIO[R, B])(implicit W: Clean.Aux[B, Pure]): RIO[R, Pure] =
    Task(f(value).map(W.apply)).flatten

  final protected def executeSuccess[B, C](f: V => B)(implicit W: Clean.Aux[B, C]): UIO[C] = UIO(W(f(value)))

  final protected def executeSuccessM[R, E, B, Pure](
    f: V => ZIO[R, E, B]
  )(implicit W: Clean.Aux[B, Pure]): ZIO[R, E, Pure] =
    f(value).map(W.apply)

  final protected def executeSuccessNow[B, Pure](f: V => B)(implicit W: Clean.Aux[B, Pure]): Pure = W(f(value))

  final protected def executeNow[B, Pure](f: V => B)(implicit W: Clean.Aux[B, Pure]): Try[Pure] = Try(W(f(value)))
}

abstract class ImpureF[-R, +V](rio: RIO[R, Impure[V]]) {
  final def execute[B, Pure](f: V => B)(implicit W: Clean.Aux[B, Pure]): RIO[R, Pure] = rio >>= (_.execute(f))
}
