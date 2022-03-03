package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}
import org.apache.spark.sql.execution.ExplainMode

import zio.{Console, RIO, Task, ZIO}
import zio.spark.internal.Impure.ImpureBox
import zio.spark.internal.codegen.BaseDataset

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
    extends BaseDataset(underlyingDataset) {

  /** Alias for [[tail]]. */
  def last: Task[T] = tail

  /**
   * Takes the last element of a dataset or throws an exception.
   *
   * See [[UnderlyingDataset.tail]] for more information.
   */
  def tail: Task[T] = tail(1).map(_.head)

  /** Alias for [[tailOption]]. */
  def lastOption: Task[Option[T]] = tailOption

  /** Takes the last element of a dataset or None. */
  def tailOption: Task[Option[T]] = tail(1).map(_.headOption)

  /** Alias for [[tail]]. */
  def takeRight(n: Int): Task[Seq[T]] = tail(n)

  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[UnderlyingDataset.summary]] for more information.
   */
  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame = summary(statistics.map(_.toString): _*)

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
  def explain(mode: String): RIO[SparkSession with Console, Unit] = explain(ExplainMode.fromString(mode))

  /**
   * Prints the plans (logical and physical) with a format specified by
   * a given explain mode.
   *
   * @group basic
   * @since 3.0.0
   */
  def explain(mode: ExplainMode): RIO[SparkSession with Console, Unit] =
    for {
      ss   <- ZIO.service[SparkSession]
      plan <- ss.withActive(underlyingDataset.succeedNow(_.queryExecution.explainString(mode)))
      _    <- Console.printLine(plan)
    } yield ()

}
