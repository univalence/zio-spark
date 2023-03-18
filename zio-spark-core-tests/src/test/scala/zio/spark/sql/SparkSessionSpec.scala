package zio.spark.sql

import org.apache.spark.SparkConf

import zio.spark.parameter._
import zio.spark.test.defaultSparkSession
import zio.test._
import zio.test.TestAspect.ignore

object SparkSessionSpec extends ZIOSpecDefault {
  def spec: Spec[Annotations with Live, Any] = sparkSessionOptionsSpec + sparkSessionDefinitionsSpec + sparkSessionSpec

  def sparkSessionSpec: Spec[Annotations with Live, Throwable] =
    suite("Spark Session") {
      test("Spark Session should be properly closed") {
        val customSessionScope =
          zio.Scope.make.flatMap { s =>
            s.extend[Any](defaultSparkSession.acquireRelease)
              .onExit(e => s.close(e))
              .map(x => x.underlyingSparkSession.sparkContext.isStopped)
          }

        assertZIO(customSessionScope)(Assertion.isTrue)
      } @@ ignore // TODO find a way to execute last
    }

  def sparkSessionOptionsSpec: Spec[Annotations with Live, TestFailure[Any]] =
    suite("SparkSession Builder Options")(
      test("SparkSession Builder should apply options correctly") {
        val expected                = Map("a" -> "x", "b" -> "y")
        val sparkSessionWithConfigs = SparkSession.builder.configs(expected)

        assertTrue(sparkSessionWithConfigs.extraConfigs == expected)
      },
      test("SparkSession Builder should enable hive support correctly") {
        val sparkSessionWithConfigs = SparkSession.builder.enableHiveSupport

        assertTrue(sparkSessionWithConfigs.hiveSupport)
      },
      test("SparkSession Builder should read spark configuration") {
        val expected                = Map("spark.app.name" -> "test")
        val conf                    = new SparkConf().setAppName("test")
        val sparkSessionWithConfigs = SparkSession.builder.config(conf)

        assertTrue(sparkSessionWithConfigs.extraConfigs == expected)
      }
    )

  def sparkSessionDefinitionsSpec: Spec[Annotations with Live, TestFailure[Any]] =
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

            assertTrue(sparkSessionWithConfigs.extraConfigs == configs)
          }
        )

      tests
    }: _*)
}
