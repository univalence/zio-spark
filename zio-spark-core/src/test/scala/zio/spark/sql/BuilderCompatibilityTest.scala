package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object BuilderCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.SparkSession.Builder, zio.spark.sql.SparkSession.Builder](
      allowedNewMethods = Seq("extraConfigs", "getOrCreateLayer", "driverMemory", "builder", "configs"),
      isImpure          = false
    )
