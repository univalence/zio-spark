package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.Task
import zio.spark.impure.Impure.ImpureBox
import zio.spark.internal.codegen.BaseDataset

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
  extends BaseDataset(underlyingDataset) {
  /**
   * Computes specified statistics for numeric and string columns.
   *
   * See [[UnderlyingDataset.summary]] for more information.
   */
  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame = summary(statistics.map(_.toString): _*)
}
