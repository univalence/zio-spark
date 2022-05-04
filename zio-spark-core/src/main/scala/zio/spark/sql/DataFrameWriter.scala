package zio.spark.sql

import org.apache.spark.sql.{DataFrameWriter => UnderlyingDataFrameWriter, SaveMode}

import zio.{Task, Trace, ZIO}

final case class DataFrameWriter[T] private (
    ds:                  Dataset[T],
    mode:                SaveMode,
    options:             Map[String, String],
    partitioningColumns: Seq[String]
) {
  private def construct: UnderlyingDataFrameWriter[T] = {
    val base = ds.underlying.write.options(options).mode(mode)

    partitioningColumns match {
      case Nil  => base
      case cols => base.partitionBy(cols: _*)
    }
  }

  /** Saves a DataFrame using one of the dataframe saver. */
  private def saveUsing(f: UnderlyingDataFrameWriter[T] => Unit)(implicit trace: Trace): Task[Unit] =
    ZIO.attempt(f(construct))

  /**
   * Saves the DataFrame using the JSON format.
   *
   * See [[UnderlyingDataFrameWriter.json]] for more information.
   */
  def json(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.json(path))

  /**
   * Saves the content of the DataFrame as the specified table.
   *
   * See [[UnderlyingDataFrameWriter.saveAsTable]] for more information.
   */
  def saveAsTable(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.saveAsTable(path))

  /** Alias for [[saveAsTable]]. */
  def table(path: String)(implicit trace: Trace): Task[Unit] = saveAsTable(path)

  /**
   * Saves the DataFrame using the CSV format.
   *
   * See [[UnderlyingDataFrameWriter.csv]] for more information.
   */
  def csv(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.csv(path))

  /**
   * Saves the DataFrame using the PARQUET format.
   *
   * See [[UnderlyingDataFrameWriter.parquet]] for more information.
   */
  def parquet(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.parquet(path))

  /**
   * Saves the DataFrame using the ORC format.
   *
   * See [[UnderlyingDataFrameWriter.orc]] for more information.
   */
  def orc(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.orc(path))

  /**
   * Saves the DataFrame using the text format.
   *
   * See [[UnderlyingDataFrameWriter.text]] for more information.
   */
  def text(path: String)(implicit trace: Trace): Task[Unit] = saveUsing(_.text(path))

  /** Adds multiple options to the DataFrameWriter. */
  def options(options: Map[String, String]): DataFrameWriter[T] = this.copy(options = this.options ++ options)

  /** Adds any type of option to the DataFrameWriter. */
  private def addOption(key: String, value: Any): DataFrameWriter[T] = options(Map(key -> value.toString))

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: String): DataFrameWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Boolean): DataFrameWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Int): DataFrameWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Float): DataFrameWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Double): DataFrameWriter[T] = addOption(key, value)

  /** Adds an option to say that the file has a header. */
  def withHeader: DataFrameWriter[T] = option("header", value = true)

  /** Setups a new [[SaveMode]] for the DataFrameWriter. */
  def mode(m: SaveMode): DataFrameWriter[T] = copy(mode = m)

  /**
   * Partitions the output by the given columns on the file system.
   *
   * See [[UnderlyingDataFrameWriter.partitionBy]] for more information.
   */
  def partitionBy(colNames: String*): DataFrameWriter[T] = copy(partitioningColumns = colNames)
}

object DataFrameWriter {
  def apply[T](ds: Dataset[T]): DataFrameWriter[T] =
    DataFrameWriter(
      ds                  = ds,
      mode                = SaveMode.ErrorIfExists,
      options             = Map.empty,
      partitioningColumns = Seq.empty
    )
}
