package zio.spark.sql

import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame, DataFrameReader => UnderlyingDataFrameReader}

import zio._

final case class DataFrameReader(reader: UnderlyingDataFrameReader, extraOptions: Map[String, String] = Map()) {

  /**
   * Load a dataframe from a CSV file.
   *
   * See [[UnderlyingDataFrameReader.csv]] for more information.
   */
  def csv(path: String): Task[DataFrame] = loadUsing(_.csv(path))

  /** Load a dataframe using one of the dataframe loader. */
  def loadUsing(f: UnderlyingDataFrameReader => UnderlyingDataFrame): Task[DataFrame] =
    Task.attemptBlocking(Dataset(f(reader.options(extraOptions))))

  /** Add multiple options to the DataFrameReader. */
  def options(options: Map[String, String]): DataFrameReader = copy(reader, extraOptions ++ options)

  /** Add an option to delimit the column from a csv file */
  def withDelimiter(delimiter: String): DataFrameReader = option("delimiter", delimiter)

  /** Add an option to say that the file has a header */
  def withHeader: DataFrameReader = option("header", value = true)

  /** Add an option to say that spark should infer the schema */
  def inferSchema: DataFrameReader = option("inferSchema", value = true)

  /** Add an option to the DataFrameReader */
  def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader (for Int) */
  def option(key: String, value: Int): DataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader (for Float) */
  def option(key: String, value: Float): DataFrameReader = option(key, value.toString)

  /** Add an option to the DataFrameReader */
  def option(key: String, value: String): DataFrameReader = copy(reader, extraOptions + (key -> value))

  /** Add an option to the DataFrameReader (for Double) */
  def option(key: String, value: Double): DataFrameReader = option(key, value.toString)
}
