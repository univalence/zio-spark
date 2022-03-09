package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween

object DataFrameReaderCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.DataFrameReader, zio.spark.sql.DataFrameReader](
      allowedNewMethods = Seq("withHeader", "withDelimiter", "inferSchema")
    )
