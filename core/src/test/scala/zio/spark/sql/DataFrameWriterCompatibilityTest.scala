package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object DataFrameWriterCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.DataFrameWriter[Any], zio.spark.sql.DataFrameWriter[Any]](
      allowedNewMethods = Seq("source", "sourceMode", "ds"),
      isImpure          = false
    )
