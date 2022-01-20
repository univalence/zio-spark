package zio.spark.sql

import zio._
import org.apache.spark.sql.{DataFrameReader => UnderlyingDataFrameReader}

final case class ZDataFrameReader(raw: UnderlyingDataFrameReader) extends DataFrameReader {
  override def csv(path: String): Task[DataFrame] = Task.attempt(ZDataFrame(raw.csv(path)))
}