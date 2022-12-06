package zio.spark.sql

import zio.spark.helper._
import zio.spark.sql.Statistics._

object StatisticsSpec
    extends ADTTestFor[Statistics](
      name = "Statistics",
      conftests =
        List(
          Conftest("count", Count, "count"),
          Conftest("mean", Mean, "mean"),
          Conftest("stddev", Stddev, "stddev"),
          Conftest("min", Min, "min"),
          Conftest("max", Max, "max"),
          Conftest("approximate percentile", ApproximatePercentile(25), "25%"),
          Conftest("count distinct", CountDistinct, "count_distinct"),
          Conftest("approximate count distinct", ApproximateCountDistinct, "approx_count_distinct")
        )
    )
