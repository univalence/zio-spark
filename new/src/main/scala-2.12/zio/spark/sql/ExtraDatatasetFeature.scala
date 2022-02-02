package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}
import zio.spark.impure.Impure

trait ExtraDatatasetFeature[T] extends Impure[UnderlyingDataset[T]] {}
