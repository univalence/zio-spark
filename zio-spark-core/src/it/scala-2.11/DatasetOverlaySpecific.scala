import org.apache.spark.sql.execution.command.ExplainCommand

import zio._
import zio.spark.sql._

/** Handmade functions for Dataset shared for one Scala version. */
class DatasetOverlaySpecific[T](self: Dataset[T]) {
  import self._

  // template:on
  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[UnderlyingDataset.summary]] for more information.
   */
  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame =
    self.summary(statistics.map(_.toString): _*)

  /**
   * Prints the plans (logical and physical) to the console for
   * debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(extended: Boolean)(implicit trace: ZTraceElement): RIO[SparkSession with Console, Unit] = {
    val queryExecution = underlying.queryExecution
    val explain        = ExplainCommand(queryExecution.logical, extended = extended)

    for {
      rows <- SparkSession.attempt(_.sessionState.executePlan(explain).executedPlan.executeCollect())
      _    <- ZIO.foreach(rows)(r => Console.printLine(r.getString(0)))
    } yield ()
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(implicit trace: ZTraceElement): SRIO[Console, Unit] = explain(extended = false)

  /**
   * Prints the schema to the console in a nice tree format.
   *
   * @group basic
   * @since 1.6.0
   */
  def printSchema(implicit trace: ZTraceElement): RIO[Console, Unit] = Console.printLine(schema.treeString)
  // template:off
}
