package zio.spark.rdd

import org.apache.spark.rdd.{RDD => UnderlyingRDD}

import zio.Task
import zio.spark.impure.Impure

final case class RDD[T](rdd: UnderlyingRDD[T]) extends Impure[UnderlyingRDD[T]](rdd) {

  /** Applies an action to the underlying RDD. */
  def action[A](f: UnderlyingRDD[T] => A): Task[A] = Task.attemptBlocking(f(rdd))

  /** Applies a transformation to the underlying RDD. */
  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = RDD(f(rdd))

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
}
