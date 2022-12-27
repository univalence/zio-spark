package zio.spark.sql

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode}

import zio._
import zio.spark.helper.Fixture._
import zio.spark.test._
import zio.stream.{ZSink, ZStream}
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import java.nio.file.{Files, Paths}

object DataFrameWriterSpec extends SharedZIOSparkSpecDefault {

  /** Deletes the folder generated by the test. */
  def deleteGeneratedFolder(path: String): Task[Unit] = ZIO.attempt(FileUtils.deleteDirectory(Paths.get(path).toFile))

  def dataFrameWriterBuilderSpec: Spec[Live with SparkSession, Any] =
    suite("DataFrameWriter Builder Spec")(
      test("DataFrameWriter can change its mode") {
        val mode         = SaveMode.Overwrite
        val path: String = s"$targetsPath/test/output/dataframe-writer-mode"

        for {
          df <- read
          writer = df.write.mode(mode)
          _ <- writer.csv(path)
          _ <- writer.csv(path)
          _ <- deleteGeneratedFolder(path)
        } yield assertTrue(writer.mode(mode).mode == mode)
      },
      test("DataFrameWriter can change its partitions") {
        val partitionCol: String = "name"
        val path: String         = s"$targetsPath/test/output/dataframe-writer-partitionBy"

        for {
          df <- read
          writer = df.write.partitionBy(partitionCol)
          _     <- writer.csv(path)
          files <- ZIO.attempt(Files.walk(Paths.get(path)))
          isPartitioned <-
            ZStream
              .fromJavaStream(files)
              .run(ZSink.collectAll)
              .map(_.exists(_.getFileName.toString.startsWith(s"$partitionCol=")))
          _ <- deleteGeneratedFolder(path)
        } yield assertTrue(writer.partitioningColumns == Seq(partitionCol), isPartitioned)
      }
    )

  def dataFrameWriterSavingSpec: Spec[Annotations with SparkSession, Any] = {
    def writerTest(extension: String, readAgain: String => SIO[DataFrame], write: String => DataFrame => Task[Unit]) =
      test(s"DataFrameWriter can save a DataFrame to $extension") {
        val path: String = s"$targetsPath/test/output/dataframe-writer-$extension"

        for {
          _      <- read.flatMap(write(path))
          df     <- readAgain(path)
          output <- df.count
          _      <- deleteGeneratedFolder(path)
        } yield assertTrue(output == 4L)

      }

    val tests =
      List(
        writerTest(
          extension = "csv",
          readAgain = path => readCsv(path),
          write     = path => _.write.withHeader.csv(path)
        ),
        // Not working with mac M1 in old version of Spark due to snappy-java:
        // (Caused by: org.xerial.snappy.SnappyError: [FAILED_TO_LOAD_NATIVE_LIBRARY]
        // no native library is found for os.name=Mac and os.arch=aarch64)
        writerTest(
          extension = "parquet",
          readAgain = path => SparkSession.read.parquet(path),
          write     = path => _.write.parquet(path)
        ) @@ scala211(os(!_.isMac)),
        writerTest(
          extension = "orc",
          readAgain = path => SparkSession.read.orc(path),
          write     = path => _.write.orc(path)
        ),
        writerTest(
          extension = "json",
          readAgain = path => SparkSession.read.json(path),
          write     = path => _.write.json(path)
        )
      )

    suite("DataFrameWriter Saving Formats")(tests: _*)
  }

  def dataFrameWriterOptionDefinitionsSpec: Spec[Live with SparkSession, Any] = {
    def writerTest(
        testName: String,
        endo: DataFrameWriter[Row] => DataFrameWriter[Row],
        expectedKey: String,
        expectedValue: String
    ) =
      test(s"DataFrameWriter can add the option ($testName)") {
        for {
          df <- read
          write             = df.write
          writerWithOptions = endo(write)
          options           = Map(expectedKey -> expectedValue)
        } yield assert(writerWithOptions.options)(equalTo(options))
      }

    val tests =
      List(
        writerTest(
          testName      = "Any option with a boolean value",
          endo          = _.option("a", value = true),
          expectedKey   = "a",
          expectedValue = "true"
        ),
        writerTest(
          testName      = "Any option with a int value",
          endo          = _.option("a", 1),
          expectedKey   = "a",
          expectedValue = "1"
        ),
        writerTest(
          testName      = "Any option with a float value",
          endo          = _.option("a", 1f),
          expectedKey   = "a",
          expectedValue = "1.0"
        ),
        writerTest(
          testName      = "Any option with a double value",
          endo          = _.option("a", 1d),
          expectedKey   = "a",
          expectedValue = "1.0"
        ),
        writerTest(
          testName      = "Option that read header",
          endo          = _.withHeader,
          expectedKey   = "header",
          expectedValue = "true"
        ),
        test("DataFrameWriter can add multiple options") {
          val options =
            Map(
              "a" -> "b",
              "c" -> "d"
            )

          for {
            df <- read
            writer = df.write
          } yield assertTrue(writer.options(options).options == options)
        },
        test("DataFrameWriter can save a df as table") {
          for {
            df   <- read
            uuid <- Random.nextUUID
            tableName = s"save_as_table_test_$uuid".replace("-", "_")
            _         <- df.write.copy(mode = SaveMode.Overwrite).saveAsTable(tableName)
            loadDf    <- SparkSession.read.table(tableName)
            assertion <- df.except(loadDf).isEmpty
          } yield assertTrue(assertion)
        } @@ withLiveRandom,
        test("DataFrameWriter can insert a df into a table") {
          for {
            df   <- read
            uuid <- Random.nextUUID
            tableName = s"insert_into_test_$uuid".replace("-", "_")
            count  <- df.count
            _      <- df.write.copy(mode = SaveMode.Overwrite).saveAsTable(tableName)
            _      <- df.write.insertInto(tableName)
            loadDf <- SparkSession.read.table(tableName)
            output <- loadDf.count
          } yield assertTrue(output == count * 2)
        } @@ withLiveRandom
      )

    suite("DataFrameWriter Option Definitions")(tests: _*)
  }

  override def spec: Spec[Live with SparkSession with Annotations, Any] =
    suite("DataFrameWriter tests")(
      dataFrameWriterBuilderSpec,
      dataFrameWriterSavingSpec,
      dataFrameWriterOptionDefinitionsSpec
    )
}
