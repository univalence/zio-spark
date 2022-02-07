package zio.spark.rdd

import org.apache.spark.rdd.{RDD => UnderlyingRDD}

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox

import scala.reflect.ClassTag

final case class RDD[T](underlyingRDD: ImpureBox[UnderlyingRDD[T]]) extends Impure[UnderlyingRDD[T]](underlyingRDD) {
  import underlyingRDD._

  /** Applies an action to the underlying RDD. */
  def action[U](f: UnderlyingRDD[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying RDD. */
  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = succeedNow(f.andThen(x => RDD(x)))

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
   *
   * See [[UnderlyingRDD.map]] for more information.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = transformation(_.map(f))
}
