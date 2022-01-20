package zio.spark.sql

import zio._

trait DataFrameReader {
  def csv(path: String): Task[DataFrame]
}
