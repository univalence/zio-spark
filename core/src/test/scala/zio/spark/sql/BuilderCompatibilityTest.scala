package zio.spark.sql

import zio.spark.CompatibilityTestBetween

object BuilderCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.SparkSession.Builder, zio.spark.sql.SparkSession.Builder](
      Seq("extraConfigs", "getOrCreateLayer", "driverMemory", "builder", "configs"),
      false
    )
