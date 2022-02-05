package zio.spark.rdd

import org.apache.spark.rdd.{RDD => UnderlyingRDD}

import zio.Task
import zio.spark.impure.Impure

import scala.reflect.ClassTag

final case class RDD[T](private var rdd: UnderlyingRDD[T]) extends Impure[UnderlyingRDD[T]](rdd) {
  // TODO : find another way by creating an impureWrapper to hold the element and use secondary constructors
  rdd = null // trash the rdd in the local scope, force the use of Impure

  /** Applies an action to the underlying RDD. */
  def action[U](f: UnderlyingRDD[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying RDD. */
  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = succeedNow(f.andThen(RDD.apply))

  /**
   * Counts the number of elements of a RDD.
   *
   * See [[UnderlyingRDD.count]] for more information.
   */
  def count: Task[Long] = action(_.count())

  /**
   * Retrieves the elements of a RDD as a list of elements.
   *
   * See [[UnderlyingRDD.collect]] for more information.
   */
  def collect: Task[Seq[T]] = action(_.collect().toSeq)

  /**
   * Return a new RDD by applying a function to all elements of this
   * RDD.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = transformation(_.map(f))
}
