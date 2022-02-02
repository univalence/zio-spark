package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.Task
import zio.spark.impure.Impure

trait ExtraDatatasetFeature[T] extends Impure[UnderlyingDataset[T]] {

  /**
   * Takes the n last elements of a dataset.
   *
   * See [[UnderlyingDataset.tail]] for more information.
   */
  final def tail(n: Int): Task[Seq[T]] = executeBlocking(_.tail(n).toSeq)

  /** Alias for [[tail]]. */
  def last: Task[T] = tail

  /**
   * Takes the last element of a dataset or throws an exception.
   *
   * See [[UnderlyingDataset.tail]] for more information.
   */
  def tail: Task[T] = tail(1).map(_.head)

  /** Alias for [[tailOption]]. */
  def lastOption: Task[Option[T]] = tailOption

  /** Takes the last element of a dataset or None. */
  def tailOption: Task[Option[T]] = tail(1).map(_.headOption)

  /** Alias for [[tail]]. */
  def takeRight(n: Int): Task[Seq[T]] = tail(n)
}
