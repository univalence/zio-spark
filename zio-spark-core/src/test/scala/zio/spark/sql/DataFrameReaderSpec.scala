package zio.spark.sql

import zio.spark.ZioSparkTestSpec.SparkTestSpec
import zio.spark.helper.Fixture._
import zio.spark.sql.DataFrameReader.WithoutSchema
import zio.spark.sql.implicits._
import zio.test._
import zio.test.TestAspect._

object DataFrameReaderSpec extends ZIOSpecDefault {
  val reader: DataFrameReader[WithoutSchema] = SparkSession.read

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    dataFrameReaderOptionsSpec + dataFrameReaderOptionDefinitionsSpec

  def dataFrameReaderOptionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameReader Options")(
      test("DataFrameReader should apply options correctly") {
        val options           = Map("a" -> "x", "b" -> "y")
        val readerWithOptions = reader.options(options)

        assertTrue(readerWithOptions.options == options)
      }
    )

  def dataFrameReaderReadingSpec: SparkTestSpec =
    suite("DataFrameReader reading files")(
      test("DataFrameReader can read a CSV file") {
        for {
          df     <- read
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataFrameReader can read a JSON file") {
        for {
          df     <- SparkSession.read.option("multiline", "true").json(s"$resourcesPath/data.json")
          output <- df.count
        } yield assertTrue(output == 4)
      } @@ scala211(ignore),
      test("DataFrameReader can read a Parquet file") {
        for {
          df     <- SparkSession.read.parquet(s"$resourcesPath/data.parquet")
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataFrameReader can read a Orc file") {
        for {
          df     <- SparkSession.read.orc(s"$resourcesPath/data.orc")
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataFrameReader can read a Text file") {
        for {
          df     <- SparkSession.read.textFile(s"$resourcesPath/data.txt")
          output <- df.flatMap(_.split(" ")).count
        } yield assertTrue(output == 4)
      }
    )

  def dataFrameReaderOptionDefinitionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = {
    final case class ReaderTest(
        testName:      String,
        endo:          DataFrameReader[WithoutSchema] => DataFrameReader[WithoutSchema],
        expectedKey:   String,
        expectedValue: String
    ) {

      def build: ZSpec[Any, Nothing] =
        test(s"DataFrameReader can add the option ($testName)") {
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
        ),
        ReaderTest(
          testName      = "Option that infer schema",
          endo          = _.inferSchema,
          expectedKey   = "inferSchema",
          expectedValue = "true"
        ),
        ReaderTest(
          testName      = "Option that read header",
          endo          = _.withHeader,
          expectedKey   = "header",
          expectedValue = "true"
        ),
        ReaderTest(
          testName      = "Option that setup delimiter",
          endo          = _.withDelimiter(";"),
          expectedKey   = "delimiter",
          expectedValue = ";"
        )
      )

    suite("DataFrameReader Option Definitions")(tests.map(_.build): _*)
  }
}
