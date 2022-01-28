package zio.spark.sql

import zio.ZIO
import zio.spark.sql.SparkSession.Builder.LocalAllNodes
import zio.test._
import zio.test.Assertion._

object DataFrameReaderTest extends DefaultRunnableSpec {
  val reader: ZIO[Any, Throwable, DataFrameReader] = SparkSession.builder.master(LocalAllNodes).getOrCreate.map(_.read)

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    zDataFrameReaderOptionsSpec + zDataFrameReaderOptionDefinitionsSpec

  def zDataFrameReaderOptionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameReader Options")(
      test("DataFrameReader should apply options correctly") {
        val options           = Map("a" -> "x", "b" -> "y")
        val readerWithOptions = reader.map(_.options(options))

        readerWithOptions.map(r => assert(r.extraOptions)(equalTo(options)))
      }
    )

  def zDataFrameReaderOptionDefinitionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameReader Option Definitions")({
      case class OptionDefinitionTest(
          text:        String,
          f:           DataFrameReader => DataFrameReader,
          keyOutput:   String,
          valueOutput: String
      )

      val conftests =
        List(
          OptionDefinitionTest("Any option with a boolean value", _.option("a", value = true), "a", "true"),
          OptionDefinitionTest("Any option with a int value", _.option("a", 1), "a", "1"),
          OptionDefinitionTest("Any option with a float value", _.option("a", 1f), "a", "1.0"),
          OptionDefinitionTest("Any option with a double value", _.option("a", 1d), "a", "1.0"),
          OptionDefinitionTest("Option that infer schema", _.inferSchema, "inferSchema", "true"),
          OptionDefinitionTest("Option that read header", _.withHeader, "header", "true"),
          OptionDefinitionTest("Option that setup delimiter", _.withDelimiter(";"), "delimiter", ";")
        )

      val tests =
        conftests.map(conftest =>
          test(s"DataFrameReader can add the option (${conftest.text})") {
            val readerWithOptions = reader.map(conftest.f(_))
            val options           = Map(conftest.keyOutput -> conftest.valueOutput)

            readerWithOptions.map(r => assert(r.extraOptions)(equalTo(options)))
          }
        )

      tests
    }: _*)
}
