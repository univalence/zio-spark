package zio.spark.sql

import org.apache.spark.sql.{
  Column,
  Dataset => UnderlyingDataset,
  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
  Sniffer
}

import zio._
import zio.spark.internal.Impure.ImpureBox
import zio.spark.rdd.RDD

final case class Dataset[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
    extends ExtraDatasetFeature[T](underlyingDataset) {
  import underlyingDataset._

  /** Transforms the Dataset into a RelationalGroupedDataset. */
  def group(f: UnderlyingDataset[T] => UnderlyingRelationalGroupedDataset): RelationalGroupedDataset =
    succeedNow(f.andThen(x => RelationalGroupedDataset(x)))

  /** Alias for [[filter]]. */
  def where(f: T => Boolean): Dataset[T] = filter(f)

  /**
   * Displays the top rows of Dataset in a tabular form. Strings with
   * more than 20 characters will be truncated.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(numRows: Int): ZIO[Console, Throwable, Unit] = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of Dataset in a tabular form. Strings with
   * more than 20 characters will be truncated.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show: ZIO[Console, Throwable, Unit] = show(20)

  /**
   * Displays the top 20 rows of Dataset in a tabular form.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(truncate: Boolean): ZIO[Console, Throwable, Unit] = show(20, truncate)

  /**
   * Displays the top rows of Dataset in a tabular form.
   *
   * See [[UnderlyingDataset.show]] for more information.
   */
  def show(numRows: Int, truncate: Boolean): ZIO[Console, Throwable, Unit] = {
    val trunc         = if (truncate) 20 else 0
    val stringifiedDf = underlyingDataset.succeedNow(d => Sniffer.datasetShowString(d, numRows, truncate = trunc))
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
  def unpersistBlocking: UIO[Dataset[T]] = succeed(ds => Dataset(ds.unpersist(blocking = true)))

  /** Alias for [[headOption]]. */
  def firstOption: Task[Option[T]] = headOption

  /** Takes the first element of a dataset or None. */
  def headOption: Task[Option[T]] = head(1).map(_.headOption)

  /**
   * Transform the dataset into a [[RDD]].
   *
   * See [[UnderlyingDataset.rdd]] for more information.
   */
  def rdd: RDD[T] = RDD(succeedNow(_.rdd))

  /**
   * Chains custom transformations.
   *
   * See [[UnderlyingDataset.transform]] for more information.
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /** Create a DataFrameWrite from this dataset. */
  def write: DataFrameWriter[T] = DataFrameWriter(this)
}

object Dataset {
  def apply[T](underlyingDataset: UnderlyingDataset[T]): Dataset[T] = Dataset(ImpureBox(underlyingDataset))
}
