package zio.spark.sql.streaming

import zio.spark.sql.SparkSession
import zio.test._
import zio.test.Assertion.{isLeft, isRight}

object DataStreamReaderSpec extends ZIOSpecDefault {
  val reader: DataStreamReader = SparkSession.readStream

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    dataStreamReaderConfigurationsSpec + dataStreamReaderOptionDefinitionsSpec

  def dataStreamReaderConfigurationsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataStreamReader Configurations")(
      test("DataStreamReader should apply options correctly") {
        val options           = Map("a" -> "x", "b" -> "y")
        val readerWithOptions = reader.options(options)

        assertTrue(readerWithOptions.options == options)
      },
      test("DataStreamReader can use a schemaString") {
        val schema      = "firstName STRING, age STRING"
        val maybeReader = SparkSession.readStream.option("multiline", "true").schema(schema)
        assert(maybeReader)(isRight)
      },
      test("DataStreamReader handle errors with schemaString") {
        val schema      = "NOT A SCHEMA"
        val maybeReader = SparkSession.readStream.option("multiline", "true").schema(schema)
        assert(maybeReader)(isLeft)
      }
    )

  def dataStreamReaderOptionDefinitionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = {
    final case class ReaderTest(
        testName:      String,
        endo:          DataStreamReader => DataStreamReader,
        expectedKey:   String,
        expectedValue: String
    ) {

      def build: ZSpec[Any, Nothing] =
        test(s"DataStreamReader can add the option ($testName)") {
          val readerWithOptions = endo(reader)
          val options           = Map(expectedKey -> expectedValue)

          assertTrue(readerWithOptions.options == options)
        }
    }

    val tests =
      List(
        ReaderTest(
          testName      = "Any option with a boolean value",
          endo          = _.option("a", value = true),
          expectedKey   = "a",
          expectedValue = "true"
        ),
        ReaderTest(
          testName      = "Any option with a int value",
          endo          = _.option("a", 1),
          expectedKey   = "a",
          expectedValue = "1"
        ),
        ReaderTest(
          testName      = "Any option with a float value",
          endo          = _.option("a", 1f),
          expectedKey   = "a",
          expectedValue = "1.0"
        ),
        ReaderTest(
          testName      = "Any option with a double value",
          endo          = _.option("a", 1d),
          expectedKey   = "a",
          expectedValue = "1.0"
        )
      )

    suite("DataStreamReader Option Definitions")(tests.map(_.build): _*)
  }
}
