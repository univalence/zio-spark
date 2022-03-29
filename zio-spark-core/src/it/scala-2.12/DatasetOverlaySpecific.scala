import org.apache.spark.sql.execution.ExplainMode

import zio._
import zio.spark.sql._

/** Handmade functions for Dataset shared for one Scala version. */
class DatasetOverlaySpecific[T](self: Dataset[T]) {
  import self._

  // template:on

  /** Alias for [[tail]]. */
  def last(implicit trace: ZTraceElement): Task[T] = tail

  /**
   * Takes the last element of a dataset or throws an exception.
   *
   * See [[Dataset.tail]] for more information.
   */
  def tail(implicit trace: ZTraceElement): Task[T] = self.tail(1).map(_.head)

  /** Alias for [[tailOption]]. */
  def lastOption(implicit trace: ZTraceElement): Task[Option[T]] = tailOption

  /** Takes the last element of a dataset or None. */
  def tailOption(implicit trace: ZTraceElement): Task[Option[T]] = self.tail(1).map(_.headOption)

  /** Alias for [[tail]]. */
  def takeRight(n: Int)(implicit trace: ZTraceElement): Task[Seq[T]] = self.tail(n)

  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[org.apache.spark.sql.Dataset.summary]] for more information.
   */
  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame =
    self.summary(statistics.map(_.toString): _*)

  /**
   * Prints the plans (logical and physical) with a format specified by
   * a given explain mode.
   *
   * @param mode
   *   specifies the expected output format of plans. <ul> <li>`simple`
   *   Print only a physical plan.</li> <li>`extended`: Print both
   *   logical and physical plans.</li> <li>`codegen`: Print a physical
   *   plan and generated codes if they are available.</li> <li>`cost`:
   *   Print a logical plan and statistics if they are available.</li>
   *   <li>`formatted`: Split explain output into two sections: a
   *   physical plan outline and node details.</li> </ul>
   * @group basic
   * @since 3.0.0
   */
  def explain(mode: String)(implicit trace: ZTraceElement): SRIO[Console, Unit] = explain(ExplainMode.fromString(mode))

  /**
   * Prints the plans (logical and physical) with a format specified by
   * a given explain mode.
   *
   * @group basic
   * @since 3.0.0
   */
  def explain(mode: ExplainMode)(implicit trace: ZTraceElement): SRIO[Console, Unit] =
    for {
      ss   <- ZIO.service[SparkSession]
      plan <- ss.withActive(underlyingDataset.queryExecution.explainString(mode))
      _    <- Console.printLine(plan)
    } yield ()

  /**
   * Prints the schema to the console in a nice tree format.
   *
   * @group basic
   * @since 1.6.0
   */
  def printSchema(implicit trace: ZTraceElement): RIO[Console, Unit] = printSchema(Int.MaxValue)

  /**
   * Prints the schema up to the given level to the console in a nice
   * tree format.
   *
   * @group basic
   * @since 3.0.0
   */
  def printSchema(level: Int)(implicit trace: ZTraceElement): RIO[Console, Unit] =
    Console.printLine(schema.treeString(level))

  // template:off
}
