package zio.spark.sql

import zio.spark.CompatibilityTestBetween

object DataFrameReaderCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.sql.DataFrameReader, zio.spark.sql.DataFrameReader](
      Seq("withHeader", "withDelimiter", "inferSchema"),
      false
    )
