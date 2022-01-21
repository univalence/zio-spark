package zio.spark.sql

import org.apache.spark.sql.{DataFrameReader => UnderlyingDataFrameReader}

import zio._

final case class ZDataFrameReader(reader: UnderlyingDataFrameReader) extends DataFrameReader {
  override def csv(path: String): Task[DataFrame] = Task.attempt(ZDataFrame(reader.csv(path)))

  def underlying: UnderlyingDataFrameReader = reader
}
