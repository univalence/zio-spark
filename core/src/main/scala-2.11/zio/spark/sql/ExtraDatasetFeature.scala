package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}
import org.apache.spark.sql.execution.command.ExplainCommand

import zio._
import zio.spark.internal.Impure.ImpureBox
import zio.spark.internal.codegen.BaseDataset

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
    extends BaseDataset(underlyingDataset) {
  import underlyingDataset._

  /**
   * Prints the plans (logical and physical) to the console for
   * debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(extended: Boolean): RIO[SparkSession with Console, Unit] = {
    val queryExecution = succeedNow(_.queryExecution)
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
}
