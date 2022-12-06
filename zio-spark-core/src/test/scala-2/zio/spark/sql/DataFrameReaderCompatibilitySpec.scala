package zio.spark.sql

import zio.spark.helper.CompatibilityTestBetween
import zio.spark.sql.DataFrameReader.WithoutSchema

object DataFrameReaderCompatibilitySpec
    extends CompatibilityTestBetween[
      org.apache.spark.sql.DataFrameReader,
      zio.spark.sql.DataFrameReader[WithoutSchema]
    ](
      allowedNewMethods = Seq("withHeader", "withDelimiter", "inferSchema", "userSpecifiedSchema")
    )
