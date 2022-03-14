import org.apache.spark.sql.execution.command.ExplainCommand

import zio._
import zio.spark.sql._

/** Handmade functions for Dataset shared for one Scala version. */
class DatasetOverlaySpecific[T](self: Dataset[T]) {
  import self._

  // template:on

  /**
   * Prints the plans (logical and physical) to the console for
   * debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(extended: Boolean): RIO[SparkSession with Console, Unit] = {
    val queryExecution = underlyingDataset.queryExecution
    val explain        = ExplainCommand(queryExecution.logical, extended = extended)

    for {
      ss   <- ZIO.service[SparkSession]
      rows <- ss.sessionState.map(_.executePlan(explain).executedPlan.executeCollect())
      _    <- ZIO.foreach(rows)(r => Console.printLine(r.getString(0)))
    } yield ()
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain: RIO[SparkSession with Console, Unit] = explain(extended = false)

  /**
   * Prints the schema to the console in a nice tree format.
   *
   * @group basic
   * @since 1.6.0
   */
  def printSchema: RIO[Console, Unit] = Console.printLine(schema.treeString)

  // template:off
}
