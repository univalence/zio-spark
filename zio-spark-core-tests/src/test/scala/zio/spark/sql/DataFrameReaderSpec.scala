package zio.spark.sql

import org.apache.spark.sql.types._
import scala3encoders.given // scalafix:ok

import zio.spark.helper.Fixture._
import zio.spark.sql.DataFrameReader.WithoutSchema
import zio.spark.sql.implicits._
import zio.spark.test._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object DataFrameReaderSpec extends ZIOSparkSpecDefault {
  val reader: DataFrameReader[WithoutSchema] = SparkSession.read

  def spec: Spec[SparkSession, Throwable] =
    suite("DataFrameReader tests")(
      dataFrameReaderOptionsSpec,
      dataFrameReaderOptionDefinitionsSpec,
      dataFrameReaderReadingSpec
    )

  def dataFrameReaderOptionsSpec: Spec[Any, Nothing] =
    suite("DataFrameReader Options")(
      test("DataFrameReader should apply options correctly") {
        val options           = Map("a" -> "x", "b" -> "y")
        val readerWithOptions = reader.options(options)

        assert(readerWithOptions.options)(equalTo(options))
      }
    )

  def dataFrameReaderReadingSpec: Spec[SparkSession, Throwable] =
    suite("DataFrameReader reading files")(
      test("DataFrameReader can read a CSV file") {
        for {
          df     <- read
          output <- df.count
        } yield assertTrue(output == 4L)
      },
      test("DataFrameReader can read a JSON file") {
        for {
          df     <- SparkSession.read.option("multiline", "true").json(s"$resourcesPath/data-json")
          output <- df.count
        } yield assertTrue(output == 4L)
      } @@ scala211(ignore),
      test("DataFrameReader can read a Parquet file") {
        for {
          df     <- SparkSession.read.parquet(s"$resourcesPath/data-parquet")
          output <- df.count
        } yield assertTrue(output == 4L)
      },
      test("DataFrameReader can read a Orc file") {
        for {
          df     <- SparkSession.read.orc(s"$resourcesPath/data-orc")
          output <- df.count
        } yield assertTrue(output == 4L)
      },
      test("DataFrameReader can read a Text file") {
        for {
          df     <- SparkSession.read.textFile(s"$resourcesPath/data-txt")
          output <- df.flatMap(_.split(" ")).count
        } yield assertTrue(output == 4L)
      },
      test("DataFrameReader can have a schema by default") {
        val schema =
          StructType(
            Seq(
              StructField("firstName", StringType, nullable = false),
              StructField("age", IntegerType, nullable      = false)
            )
          )

        for {
          df <- SparkSession.read.option("multiline", "true").schema(schema).json(s"$resourcesPath/data-json")
        } yield assertTrue(df.columns == Seq("firstName", "age"))
      },
      test("DataFrameReader can have a schema from a case class") {
        final case class Schema(firstName: String, age: Int)

        for {
          df <- SparkSession.read.option("multiline", "true").schema[Schema].json(s"$resourcesPath/data-json")
        } yield assertTrue(df.columns == Seq("firstName", "age"))
      },
      test("DataFrameReader can use a schemaString") {
        val schema      = "firstName STRING, age STRING"
        val maybeReader = SparkSession.read.option("multiline", "true").schema(schema)
        assert(maybeReader)(isRight)
      },
      test("DataFrameReader handle errors with schemaString") {
        val schema      = "NOT A SCHEMA"
        val maybeReader = SparkSession.read.option("multiline", "true").schema(schema)
        assert(maybeReader)(isLeft)
      }
    )

  def dataFrameReaderOptionDefinitionsSpec: Spec[Any, Nothing] = {
    final case class ReaderTest(
        testName:      String,
        endo:          DataFrameReader[WithoutSchema] => DataFrameReader[WithoutSchema],
        expectedKey:   String,
        expectedValue: String
    ) {

      def build: Spec[Any, Nothing] =
        test(s"DataFrameReader can add the option ($testName)") {
          val readerWithOptions = endo(reader)
          val options           = Map(expectedKey -> expectedValue)

          assert(readerWithOptions.options)(equalTo(options))
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
