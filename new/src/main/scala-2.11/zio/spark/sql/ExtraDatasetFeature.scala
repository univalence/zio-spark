package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.spark.impure.Impure

trait ExtraDatasetFeature[T] extends Impure[UnderlyingDataset[T]] {}
