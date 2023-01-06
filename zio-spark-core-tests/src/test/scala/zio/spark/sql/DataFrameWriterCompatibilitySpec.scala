package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object DataFrameWriterCompatibilitySpec
    extends CompatibilityTestBetween[org.apache.spark.sql.DataFrameWriter[Any], zio.spark.sql.DataFrameWriter[Any]](
      allowedNewMethods = Seq("table", "partitioningColumns", "withHeader", "ds", "saveUsing")
    )
