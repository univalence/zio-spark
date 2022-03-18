package zio.spark.sql

import zio.spark.helper._
import zio.spark.sql.DataFrameWriter.Source

object DataFrameWriterSourceSpec
    extends ADTTestFor[Source](
      name = "Source",
      conftests =
        List(
          Conftest("csv", Source.CSV, "csv"),
          Conftest("parquet", Source.Parquet, "parquet"),
          Conftest("json", Source.JSON, "json"),
          Conftest("text", Source.Text, "text")
        )
    )
