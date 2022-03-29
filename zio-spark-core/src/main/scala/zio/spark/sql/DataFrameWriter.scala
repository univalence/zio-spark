package zio.spark.sql

import org.apache.spark.sql.{DataFrameWriter => UnderlyingDataFrameWriter, SaveMode}

import zio.Task
import zio.spark.sql.DataFrameWriter.Source

final case class DataFrameWriter[T](
    ds:                  Dataset[T],
    source:              Source,
    mode:                SaveMode,
    options:             Map[String, String],
    partitioningColumns: Seq[String]
) {

  private def construct: UnderlyingDataFrameWriter[T] = {
    val base = ds.underlying.write.options(options).format(source.toString).mode(mode)

    partitioningColumns match {
      case Nil  => base
      case cols => base.partitionBy(cols: _*)
    }
  }

  /** Saves a DataFrame using one of the dataframe saver. */
  private def saveUsing(f: UnderlyingDataFrameWriter[T] => Unit): Task[Unit] = Task.attempt(f(construct))

  /** Saves the content of the DataFrame as the specified table. */
  def save: Task[Unit] = saveUsing(_.save())

  /** Saves the content of the DataFrame at the specified path. */
  def save(path: String): Task[Unit] = saveUsing(_.save(path))

  /** Setups a new [[Source]] for the DataFrameWriter. */
  def format(s: Source): DataFrameWriter[T] = this.copy(source = s)

  /**
   * Saves the DataFrame using the json format.
   *
   * See [[UnderlyingDataFrameWriter.json]] for more information.
   */
  def json(path: String): Task[Unit] = format(Source.JSON).save(path)

  /**
   * Saves the DataFrame using the csv format.
   *
   * See [[UnderlyingDataFrameWriter.csv]] for more information.
   */
  def csv(path: String): Task[Unit] = format(Source.CSV).save(path)

  /**
   * Saves the DataFrame using the parquet format.
   *
   * See [[UnderlyingDataFrameWriter.parquet]] for more information.
   */
  def parquet(path: String): Task[Unit] = format(Source.Parquet).save(path)

  /**
   * Saves the DataFrame using the text format.
   *
   * See [[UnderlyingDataFrameWriter.text]] for more information.
   */
  def text(path: String): Task[Unit] = format(Source.Text).save(path)

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
      source              = Source.Parquet,
      mode                = SaveMode.ErrorIfExists,
      options             = Map.empty,
      partitioningColumns = Seq.empty
    )

  sealed trait Source {
    self =>

    import Source._

    override def toString: String =
      self match {
        case CSV     => "csv"
        case Parquet => "parquet"
        case JSON    => "json"
        case Text    => "text"
      }
  }

  object Source {
    case object CSV     extends Source
    case object Parquet extends Source
    case object JSON    extends Source
    case object Text    extends Source
  }
}
