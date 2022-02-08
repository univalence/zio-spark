package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.sql.Statistics.statisticsToString

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
  extends Impure[UnderlyingDataset[T]](underlyingDataset) {
  import underlyingDataset._

  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[UnderlyingDataset.summary]] for more information.
   */
  def summary(statistics: String*): DataFrame = Dataset(succeedNow(_.summary(statistics: _*)))

  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[UnderlyingDataset.summary]] for more information.
   */
  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame =
    summary(statistics.map(statisticsToString): _*)
}
