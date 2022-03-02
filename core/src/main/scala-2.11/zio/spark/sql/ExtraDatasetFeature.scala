package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.spark.impure.Impure.ImpureBox
import zio.spark.internal.codegen.BaseDataset

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
  extends BaseDataset(underlyingDataset)
