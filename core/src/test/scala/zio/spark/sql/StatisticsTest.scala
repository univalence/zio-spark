package zio.spark.sql

import zio.test.{assert, Annotations, DefaultRunnableSpec, Live, Spec, TestFailure, TestSuccess}
import zio.test.Assertion.equalTo

object StatisticsTest extends DefaultRunnableSpec {
  val statisticsSpec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("To string representation")({
      import Statistics._

      case class Conftest(text: String, input: Statistics, output: String)

      val conftests =
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

      val tests =
        conftests.map(conftest =>
          test(s"Statistics is converted into its string representation correctly (${conftest.text})") {
            assert(Statistics.statisticsToString(conftest.input))(equalTo(conftest.output))
          }
        )

      tests
    }: _*)

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = statisticsSpec

}
