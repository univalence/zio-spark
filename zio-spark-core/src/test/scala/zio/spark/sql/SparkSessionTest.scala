package zio.spark.sql

import zio.spark.parameter._
import zio.test._
import zio.test.Assertion._

object SparkSessionTest extends DefaultRunnableSpec {
  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    sparkSessionOptionsSpec + sparkSessionDefinitionsSpec

  def sparkSessionOptionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameReader Options")(
      test("DataFrameReader should apply options correctly") {
        val configs                 = Map("a" -> "x", "b" -> "y")
        val sparkSessionWithConfigs = SparkSession.builder.configs(configs)

        assert(sparkSessionWithConfigs.extraConfigs)(equalTo(configs))
      }
    )

  def sparkSessionDefinitionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameReader Option Definitions")({
      final case class Conftest(
          text:        String,
          f:           SparkSession.Builder => SparkSession.Builder,
          keyOutput:   String,
          valueOutput: String
      )

      val conftests =
        List(
          Conftest("Any config with a boolean value", _.config("a", value = true), "a", "true"),
          Conftest("Any config with a int value", _.config("a", 1), "a", "1"),
          Conftest("Any config with a float value", _.config("a", 1f), "a", "1.0"),
          Conftest("Any config with a double value", _.config("a", 1d), "a", "1.0"),
          Conftest("Config that set the application name", _.appName("test"), "spark.app.name", "test"),
          Conftest("Config that set the master mode", _.master(localAllNodes), "spark.master", "local[*]"),
          Conftest("Config that set the driver memory", _.driverMemory(1.kb), "spark.driver.memory", "1kb")
        )

      val tests =
        conftests.map(conftest =>
          test(s"DataFrameReader can add the option (${conftest.text})") {
            val sparkSessionWithConfigs = conftest.f(SparkSession.builder)
            val configs                 = Map(conftest.keyOutput -> conftest.valueOutput)

            assert(sparkSessionWithConfigs.extraConfigs)(equalTo(configs))
          }
        )

      tests
    }: _*)
}
