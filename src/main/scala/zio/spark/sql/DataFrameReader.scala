package zio.spark.sql

import zio._

trait DataFrameReader {
  def csv(path: String): Task[DataFrame]

  def option(key: String, value: String): DataFrameReader
  def option(key: String, value: Boolean): DataFrameReader
  def option(key: String, value: Float): DataFrameReader
  def option(key: String, value: Int): DataFrameReader
  def option(key: String, value: Double): DataFrameReader

  def options(options: Map[String, String]): DataFrameReader

  def withDelimiter(delimiter: String): DataFrameReader

  def withHeader: DataFrameReader

  def inferSchema: DataFrameReader
}
