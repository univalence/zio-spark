package zio.spark.sql

import org.apache.spark.sql.{DataFrameReader => UnderlyingDataFrameReader}

import zio._

final case class ZDataFrameReader(raw: UnderlyingDataFrameReader) extends DataFrameReader {
  override def csv(path: String): Task[DataFrame] = Task.attempt(ZDataFrame(raw.csv(path)))
}
