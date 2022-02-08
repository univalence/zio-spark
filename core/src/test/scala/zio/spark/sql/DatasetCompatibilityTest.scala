package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween
import zio.spark.sql.ExtraDatasetCompatibilityMethods._

object DatasetCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.Dataset[Any], zio.spark.sql.Dataset[Any]](
      allowedNewMethods =
        Seq(
          "underlyingDataset",
          "transformation",
          "action",
          "headOption",
          "firstOption",
          "unpersistBlocking"
        ) ++ extraAllowedMethods,
      isImpure = true
    )
