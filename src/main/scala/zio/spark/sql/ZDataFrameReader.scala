package zio.spark.sql

import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame, DataFrameReader => UnderlyingDataFrameReader}

import zio._

final case class ZDataFrameReader(reader: UnderlyingDataFrameReader, extraOptions: Map[String, String] = Map())
    extends DataFrameReader {
  override def csv(path: String): Task[DataFrame] = extension(_.csv(path))

  def extension(f: UnderlyingDataFrameReader => UnderlyingDataFrame): Task[DataFrame] =
    Task.attemptBlocking(ZDataFrame(f(reader.options(extraOptions))))

  /** Add multiple options to the DataFrameReader. */
  override def options(options: Map[String, String]): ZDataFrameReader = copy(reader, extraOptions ++ options)

  /** Add an option to delimit the column from a csv file */
  override def withDelimiter(delimiter: String): ZDataFrameReader = option("delimiter", delimiter)

  /** Add an option to the DataFrameReader */
  override def option(key: String, value: String): ZDataFrameReader = copy(reader, extraOptions + (key -> value))

  /** Add an option to say that the file has a header */
  override def withHeader: ZDataFrameReader = option("header", value = true)

  /** Add an option to say that spark should infer the schema */
  override def inferSchema: ZDataFrameReader = option("inferSchema", value = true)

  /** Add an option to the DataFrameReader */
  override def option(key: String, value: Boolean): ZDataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader (for Int) */
  override def option(key: String, value: Int): ZDataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader (for Float) */
  override def option(key: String, value: Float): ZDataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader (for Double) */
  override def option(key: String, value: Double): ZDataFrameReader = option(key, value.toString)

  def underlying: UnderlyingDataFrameReader = reader
}
