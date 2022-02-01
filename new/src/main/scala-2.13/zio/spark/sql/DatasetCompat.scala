package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

object DatasetCompat {
  def tail[T](ds: UnderlyingDataset[T], n: Int): List[T] = ds.tail(n).toList
}
