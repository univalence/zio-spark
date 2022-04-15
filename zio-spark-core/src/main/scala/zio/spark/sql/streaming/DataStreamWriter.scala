package zio.spark.sql.streaming

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.{
  DataStreamWriter => UnderlyingDataStreamWriter,
  OutputMode,
  StreamingQuery,
  Trigger
}

import zio.{Duration, Task, ZIO}
import zio.spark.sql.Dataset

import java.util.concurrent.TimeoutException

final case class DataStreamWriter[T] private (
    ds:                  Dataset[T],
    options:             Map[String, String],
    source:              Option[String],
    outputMode:          OutputMode,
    trigger:             Trigger,
    partitioningColumns: Option[Seq[String]],
    foreachWriter:       Option[ForeachWriter[T]]
) { self =>
  implicit private class UnderlyingDataStreamWriterAddons(writer: UnderlyingDataStreamWriter[T]) {
    def maybeUse[U](
        maybeValue: Option[U],
        f: UnderlyingDataStreamWriter[T] => U => UnderlyingDataStreamWriter[T]
    ): UnderlyingDataStreamWriter[T] =
      maybeValue match {
        case None        => writer
        case Some(value) => f(writer)(value)
      }
  }

  private def construct: UnderlyingDataStreamWriter[T] =
    ds.underlying.writeStream
      .options(options)
      .maybeUse(source, _.format)
      .outputMode(outputMode)
      .trigger(trigger)
      .maybeUse(partitioningColumns, _.partitionBy)
      .maybeUse(foreachWriter, _.foreach)

  /** Adds multiple options to the DataFrameWriter. */
  def options(options: Map[String, String]): DataStreamWriter[T] = this.copy(options = this.options ++ options)

  /** Adds any type of option to the DataFrameWriter. */
  private def addOption(key: String, value: Any): DataStreamWriter[T] = options(Map(key -> value.toString))

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: String): DataStreamWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Boolean): DataStreamWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Int): DataStreamWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Float): DataStreamWriter[T] = addOption(key, value)

  /** Adds an option to the DataFrameWriter. */
  def option(key: String, value: Double): DataStreamWriter[T] = addOption(key, value)

  /**
   * Change the source (sink) of the stream.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamWriter[T] = copy(source = Some(source))

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a
   * streaming sink. <ul> <li> `OutputMode.Append()`: only the new rows
   * in the streaming DataFrame/Dataset will be written to the
   * sink.</li> <li> `OutputMode.Complete()`: all the rows in the
   * streaming DataFrame/Dataset will be written to the sink every time
   * there are some updates.</li> <li> `OutputMode.Update()`: only the
   * rows that were updated in the streaming DataFrame/Dataset will be
   * written to the sink every time there are some updates. If the query
   * doesn't contain aggregations, it will be equivalent to
   * `OutputMode.Append()` mode.</li> </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = copy(outputMode = outputMode)

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a
   * streaming sink. <ul> <li> `append`: only the new rows in the
   * streaming DataFrame/Dataset will be written to the sink.</li> <li>
   * `complete`: all the rows in the streaming DataFrame/Dataset will be
   * written to the sink every time there are some updates.</li> <li>
   * `update`: only the rows that were updated in the streaming
   * DataFrame/Dataset will be written to the sink every time there are
   * some updates. If the query doesn't contain aggregations, it will be
   * equivalent to `append` mode.</li> </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: String): Either[IllegalArgumentException, DataStreamWriter[T]] =
    outputMode.toLowerCase match {
      case "append"   => Right(self.outputMode(OutputMode.Append()))
      case "complete" => Right(self.outputMode(OutputMode.Complete()))
      case "update"   => Right(self.outputMode(OutputMode.Update()))
      case _ =>
        Left(
          new IllegalArgumentException(
            s"Unknown output mode $outputMode. Accepted output modes are 'append', 'complete', 'update'"
          )
        )
    }

  /**
   * Set the trigger for the stream query. The default value is
   * `ProcessingTime(0)` and it will run the query as fast as possible.
   *
   * Scala Example:
   * {{{
   *   df.writeStream.trigger(ProcessingTime("10 seconds"))
   *
   *   import scala.concurrent.duration._
   *   df.writeStream.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * @since 2.0.0
   */
  def trigger(trigger: Trigger): DataStreamWriter[T] = copy(trigger = trigger)

  /**
   * A ZIO-Spark specific function to describe the streaming trigger.
   *
   * Scala Example, using ZIO duration ops:
   * {{{
   *   df.writeStream.triggerEvery(5.seconds)
   * }}}
   */
  def triggerEvery(duration: Duration): DataStreamWriter[T] = trigger(Trigger.ProcessingTime(duration.toMillis))

  /**
   * A ZIO-Spark specific function to run the streaming job only once.
   */
  def once: DataStreamWriter[T] = trigger(Trigger.Once())

  /**
   * Specifies the name of the [[StreamingQuery]] that can be started
   * with `start()`. This name must be unique among all the currently
   * active queries in the associated SQLContext.
   *
   * @since 2.0.0
   */
  def queryName(queryName: String): DataStreamWriter[T] = addOption("queryName", queryName)

  /**
   * Partitions the output by the given columns on the file system. If
   * specified, the output is laid out on the file system similar to
   * Hive's partitioning scheme. As an example, when we partition a
   * dataset by year and then month, the directory layout would look
   * like:
   *
   * <ul> <li> year=2016/month=01/</li> <li> year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize
   * physical data layout. It provides a coarse-grained index for
   * skipping unnecessary data reads when queries have predicates on the
   * partitioned columns. In order for partitioning to work well, the
   * number of distinct values in each column should typically be less
   * than tens of thousands.
   *
   * @since 2.0.0
   */
  def partitionBy(colName: String, colNames: String*): DataStreamWriter[T] =
    copy(partitioningColumns = Some(colName +: colNames))

  /**
   * Starts the execution of the streaming query, which will continually
   * output results to the given path as new data arrives. The returned
   * [[StreamingQuery]] object can be used to interact with the stream.
   *
   * @since 2.0.0
   */
  def start(path: String): Task[StreamingQuery] = Task.attempt(construct.start(path))

  /**
   * Starts the execution of the streaming query, which will continually
   * output results to the given path as new data arrives. The returned
   * [[StreamingQuery]] object can be used to interact with the stream.
   * Throws a `TimeoutException` if the following conditions are met:
   *   - Another run of the same streaming query, that is a streaming
   *     query sharing the same checkpoint location, is already active
   *     on the same Spark Driver
   *   - The SQL configuration
   *     `spark.sql.streaming.stopActiveRunOnRestart` is enabled
   *   - The active run cannot be stopped within the timeout controlled
   *     by the SQL configuration `spark.sql.streaming.stopTimeout`
   *
   * @since 2.0.0
   */
  @throws[TimeoutException]
  def start: Task[StreamingQuery] = Task.attempt(construct.start())

  /**
   * Generate a stream with only the available current input. Generally
   * used for testing purpose.
   */
  def test: Task[Unit] = start.map(_.processAllAvailable())

  /**
   * Generate the stream as a stoppable blocking task handled by ZIO.
   */
  def run: Task[Unit] =
    for {
      query <- start
      _     <- Task.attempt(query.awaitTermination()).onInterrupt(ZIO.succeed(query.stop()))
    } yield ()

  /**
   * Sets the output of the streaming query to be processed using the
   * provided writer object. object. See
   * [[org.apache.spark.sql.ForeachWriter]] for more details on the
   * lifecycle and semantics.
   * @since 2.0.0
   */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] = copy(foreachWriter = Option(writer))
}

object DataStreamWriter {
  def apply[T](ds: Dataset[T]): DataStreamWriter[T] =
    DataStreamWriter(
      ds                  = ds,
      options             = Map.empty,
      source              = None,
      outputMode          = OutputMode.Append(),
      trigger             = Trigger.ProcessingTime(0L),
      partitioningColumns = None,
      foreachWriter       = None
    )
}
