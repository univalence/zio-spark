package zio.spark.sql

import org.apache.spark.sql.{DataFrameReader => UnderlyingDataFrameReader, Dataset => UnderlyingDataset}

final case class DataFrameReader(options: Map[String, String]) {

  /**
   * Loads a dataframe from a CSV file.
   *
   * See [[UnderlyingDataFrameReader.csv]] for more information.
   */
  def csv(path: String): Spark[DataFrame] = loadUsing(_.csv(path))

  /**
   * Loads a dataframe from a text file.
   *
   * See [[UnderlyingDataFrameReader.textFile]] for more information.
   */
  def textFile(path: String): Spark[Dataset[String]] = loadUsing(_.textFile(path))

  /** Loads a dataframe using one of the dataframe loader. */
  private def loadUsing[T](f: UnderlyingDataFrameReader => UnderlyingDataset[T]): Spark[Dataset[T]] =
    fromSpark(ss => Dataset(f(ss.read.options(options))))

  /** Adds multiple options to the DataFrameReader. */
  def options(options: Map[String, String]): DataFrameReader = DataFrameReader(this.options ++ options)

  /** Adds any type of option to the DataFrameReader. */
  private def addOption(key: String, value: Any) = DataFrameReader(this.options + (key -> value.toString))

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: String): DataFrameReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Boolean): DataFrameReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Int): DataFrameReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Float): DataFrameReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Double): DataFrameReader = addOption(key, value)

  /** Adds an option to delimit the column from a csv file. */
  def withDelimiter(delimiter: String): DataFrameReader = option("delimiter", delimiter)

  /** Adds an option to say that the file has a header. */
  def withHeader: DataFrameReader = option("header", value = true)

  /** Adds an option to say that spark should infer the schema. */
  def inferSchema: DataFrameReader = option("inferSchema", value = true)
}
