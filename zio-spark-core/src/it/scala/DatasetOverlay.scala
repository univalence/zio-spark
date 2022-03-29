import org.apache.spark.sql.{
  Column,
  Dataset => UnderlyingDataset,
  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
  Sniffer
}

import zio._
import zio.spark.rdd._
import zio.spark.sql._

/** Handmade functions for Dataset shared for all Scala versions. */
class DatasetOverlay[T](self: Dataset[T]) {
  import self._

  // template:on

  /** Transforms the Dataset into a RelationalGroupedDataset. */
  def group(f: UnderlyingDataset[T] => UnderlyingRelationalGroupedDataset): RelationalGroupedDataset =
    RelationalGroupedDataset(f(underlying))

  /** Alias for [[filter]]. */
  def where(f: T => Boolean): Dataset[T] = filter(f)

  /**
   * Displays the top rows of Dataset in a tabular form. Strings with
   * more than 20 characters will be truncated.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(numRows: Int): RIO[Console, Unit] = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of Dataset in a tabular form. Strings with
   * more than 20 characters will be truncated.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show: RIO[Console, Unit] = show(20)

  /**
   * Displays the top 20 rows of Dataset in a tabular form.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(truncate: Boolean): RIO[Console, Unit] = show(20, truncate)

  /**
   * Displays the top rows of Dataset in a tabular form.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(numRows: Int, truncate: Boolean): RIO[Console, Unit] = {
    val trunc         = if (truncate) 20 else 0
    val stringifiedDf = Sniffer.datasetShowString(underlying, numRows, truncate = trunc)
    Console.printLine(stringifiedDf)
  }

  /**
   * Groups the Dataset using the specified columns, so we ca run
   * aggregations on them.
   *
   * See [[UnderlyingDataset.groupBy]] for more information.
   */
  def groupBy(cols: Column*): RelationalGroupedDataset = group(_.groupBy(cols: _*))

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it
   * from memory and disk in a blocking way.
   *
   * See [[UnderlyingDataset.unpersist]] for more information.
   */
  def unpersistBlocking: UIO[Dataset[T]] = UIO.succeed(transformation(_.unpersist(blocking = true)))

  /** Alias for [[headOption]]. */
  def firstOption: Task[Option[T]] = headOption

  /** Takes the first element of a dataset or None. */
  def headOption: Task[Option[T]] = head(1).map(_.headOption)

  /**
   * Transform the dataset into a [[RDD]].
   *
   * See [[UnderlyingDataset.rdd]] for more information.
   */
  def rdd: RDD[T] = RDD(get(_.rdd))

  /**
   * Chains custom transformations.
   *
   * See [[UnderlyingDataset.transform]] for more information.
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(self)

  /** Create a DataFrameWrite from this dataset. */
  def write: DataFrameWriter[T] = DataFrameWriter(self)

  // template:off
}
