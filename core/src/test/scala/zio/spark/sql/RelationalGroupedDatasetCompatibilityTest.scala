package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object RelationalGroupedDatasetCompatibilityTest
    extends CompatibilityTestBetween[
      org.apache.spark.sql.RelationalGroupedDataset,
      zio.spark.sql.RelationalGroupedDataset
    ](
      allowedNewMethods = Seq("underlyingRelationalDataset"),
      isImpure          = true
    )
