package zio.spark.sql.streaming

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import zio.spark.ZioSparkTestSpec.SparkTestSpec
import zio.spark.helper.Fixture.resourcesPath
import zio.spark.sql.SparkSession
import zio.spark.sql.implicits._
import zio.test.{assert, assertTrue, Annotations, Live, Spec, TestFailure, TestSuccess, ZIOSpecDefault, ZSpec}
import zio.test.Assertion.{isLeft, isRight}
import zio.test.TestAspect.{ignore, scala211}

object DataStreamReaderSpec extends ZIOSpecDefault {
  val reader: DataStreamReader = SparkSession.readStream

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    dataStreamReaderOptionsSpec + dataStreamReaderOptionDefinitionsSpec

  def dataStreamReaderOptionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataStreamReader Options")(
      test("DataStreamReader should apply options correctly") {
        val options           = Map("a" -> "x", "b" -> "y")
        val readerWithOptions = reader.options(options)

        assertTrue(readerWithOptions.options == options)
      }
    )

  def dataStreamReaderReadingSpec: SparkTestSpec =
    suite("DataStreamReader reading files")(
      test("DataStreamReader can read a CSV file") {
        for {
          df     <- SparkSession.readStream.csv("")
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataStreamReader can read a JSON file") {
        for {
          df     <- SparkSession.read.option("multiline", "true").json(s"$resourcesPath/data.json")
          output <- df.count
        } yield assertTrue(output == 4)
      } @@ scala211(ignore),
      test("DataStreamReader can read a Parquet file") {
        for {
          df     <- SparkSession.read.parquet(s"$resourcesPath/data.parquet")
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataStreamReader can read a Orc file") {
        for {
          df     <- SparkSession.read.orc(s"$resourcesPath/data.orc")
          output <- df.count
        } yield assertTrue(output == 4)
      },
      test("DataStreamReader can read a Text file") {
        for {
          df     <- SparkSession.read.textFile(s"$resourcesPath/data.txt")
          output <- df.flatMap(_.split(" ")).count
        } yield assertTrue(output == 4)
      },
      test("DataStreamReader can have a schema by default") {
        val schema =
          StructType(
            Seq(
              StructField("firstName", StringType, nullable = false),
              StructField("age", IntegerType, nullable      = false)
            )
          )

        for {
          df <- SparkSession.read.option("multiline", "true").schema(schema).json(s"$resourcesPath/data.json")
        } yield assertTrue(df.columns == Seq("firstName", "age"))
      },
      test("DataStreamReader can use a schemaString") {
        val schema      = "firstName STRING, age STRING"
        val maybeReader = SparkSession.read.option("multiline", "true").schema(schema)
        assert(maybeReader)(isRight)
      },
      test("DataStreamReader handle errors with schemaString") {
        val schema      = "NOT A SCHEMA"
        val maybeReader = SparkSession.read.option("multiline", "true").schema(schema)
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
