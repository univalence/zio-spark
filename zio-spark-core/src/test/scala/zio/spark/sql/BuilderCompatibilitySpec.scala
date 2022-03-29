package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object BuilderCompatibilitySpec
    extends CompatibilityTestBetween[org.apache.spark.sql.SparkSession.Builder, zio.spark.sql.SparkSession.Builder](
      allowedNewMethods = Seq("configs", "asLayer", "acquireRelease", "driverMemory", "builder", "extraConfigs")
    )
