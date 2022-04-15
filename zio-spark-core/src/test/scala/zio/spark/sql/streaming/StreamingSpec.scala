package zio.spark.sql.streaming

import org.apache.spark.sql.streaming.OutputMode

import zio.spark.helper.Fixture.{resourcesPath, Person}
import zio.spark.sql._
import zio.spark.sql.{SIO, SparkSession}
import zio.test._

object StreamingSpec {

  def testStreamingPipeline(
      name: String,
      readingEffect: DataStreamReader => SIO[DataFrame]
  ): ZSpec[SparkSession, Throwable] =
    test(s"Streaming should work with ${name.toUpperCase}s") {
      val tableName = s"test${name.capitalize}"

      for {
        df <- readingEffect(SparkSession.readStream.schema[Person])
        _ <-
          df.writeStream
            .format("memory")
            .outputMode(OutputMode.Append())
            .queryName(tableName)
            .test
        memoryDf <- SparkSession.read.table(tableName)
        res      <- memoryDf.count
      } yield assertTrue(res == 4)
    }

  def streamingSpec: Spec[SparkSession, TestFailure[Throwable], TestSuccess] =
    suite("Streaming spec")(
      testStreamingPipeline(
        name          = "csv",
        readingEffect = _.option("header", value = true).option("delimiter", ";").csv(s"$resourcesPath/data-csv")
      ),
      testStreamingPipeline(
        name          = "json",
        readingEffect = _.option("multiline", "true").json(s"$resourcesPath/data-json")
      ),
      testStreamingPipeline(
        name          = "parquet",
        readingEffect = _.parquet(s"$resourcesPath/data-parquet")
      ),
      testStreamingPipeline(
        name          = "orc",
        readingEffect = _.orc(s"$resourcesPath/data-orc")
      )
    )
}
