package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}
import zio.spark.impure.Impure

abstract class ExtraDatasetFeature[T](ds: UnderlyingDataset[T]) extends Impure[UnderlyingDataset[T]](ds) {}