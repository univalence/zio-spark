package zio.spark.sql

import org.apache.spark.sql.{
  DataFrame => UnderlyingDataFrame,
  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset
}

final case class RelationalGroupedDataset(underlyingRelationalDataset: UnderlyingRelationalGroupedDataset) {

  /** Transforms the RelationalGroupedDataset into a DataFrame. */
  private def ungroup(f: UnderlyingRelationalGroupedDataset => UnderlyingDataFrame): DataFrame =
    Dataset(f(underlyingRelationalDataset))

  /**
   * Computes the average for each numeric columns.
   *
   * See [[UnderlyingRelationalGroupedDataset.avg]] for more
   * information.
   */
  def avg(colnames: String*): DataFrame = ungroup(_.avg(colnames: _*))

  /** Alias for [[avg]]. */
  def mean(colnames: String*): DataFrame = avg(colnames: _*)

  /**
   * Computes the max for each numeric columns.
   *
   * See [[UnderlyingRelationalGroupedDataset.max]] for more
   * information.
   */
  def max(colnames: String*): DataFrame = ungroup(_.max(colnames: _*))

  /**
   * Computes the min for each numeric columns.
   *
   * See [[UnderlyingRelationalGroupedDataset.min]] for more
   * information.
   */
  def min(colnames: String*): DataFrame = ungroup(_.min(colnames: _*))

  /**
   * Computes the sum for each numeric columns.
   *
   * See [[UnderlyingRelationalGroupedDataset.sum]] for more
   * information.
   */
  def sum(colnames: String*): DataFrame = ungroup(_.sum(colnames: _*))
}
