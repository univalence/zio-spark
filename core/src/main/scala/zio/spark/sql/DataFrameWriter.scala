package zio.spark.sql

import org.apache.spark.sql.{DataFrameWriter => UnderlyingDataFrameWriter, SaveMode}

import zio.Task
import zio.spark.sql.DataFrameWriter.Source

final case class DataFrameWriter[T](ds: Dataset[T], source: Source, sourceMode: SaveMode) {

  /** Saves a DataFrame using one of the dataframe saver. */
  private def saveUsing(f: UnderlyingDataFrameWriter[T] => Unit): Task[Unit] =
    Task(f(ds.underlyingDataset.succeedNow(_.write).format(source.toString).mode(sourceMode)))

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
}

object DataFrameWriter {
  def apply[T](ds: Dataset[T]): DataFrameWriter[T] =
    DataFrameWriter(
      ds,
      Source.Parquet,
      SaveMode.ErrorIfExists
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
