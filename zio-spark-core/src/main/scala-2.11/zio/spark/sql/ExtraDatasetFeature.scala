package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox

abstract class ExtraDatasetFeature[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]]) extends Impure[UnderlyingDataset[T]](underlyingDataset) {}