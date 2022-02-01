package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

object DatasetCompat {

  /**
   * A loosely way to make the old spark version compatible with the
   * tail feature of the new one.
   */
  def tail[T](ds: UnderlyingDataset[T], n: Int): List[T] = ds.collect().toList.takeRight(n)
}
