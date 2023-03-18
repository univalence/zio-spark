package zio.spark.sql.streaming

import scala3encoders.given // scalafix:ok

import zio.spark.helper.Fixture.{resourcesPath, Person}
import zio.spark.parameter.append
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test._
import zio.test._

object StreamingSpec extends ZIOSparkSpecDefault {

  def testStreamingPipeline(
      name: String,
      readingEffect: DataStreamReader => SIO[DataFrame]
  ): Spec[SparkSession, Throwable] =
    test(s"Streaming should work with ${name.toUpperCase}s") {
      val tableName = s"test${name.capitalize}"

      for {
        df <- readingEffect(SparkSession.readStream.schema[Person])
        _ <-
          df.writeStream
            .format("memory")
            .outputMode(append)
            .queryName(tableName)
            .test
        memoryDf <- SparkSession.read.table(tableName)
        res      <- memoryDf.count
      } yield assertTrue(res == 4L)
    }

  override def spec: Spec[SparkSession, Throwable] =
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
      ),
      test(s"Streaming using once trigger is the same as test") {
        for {
          df <- SparkSession.readStream.textFile(s"$resourcesPath/data-txt")
          _ <-
            df.flatMap(_.split(" "))
              .writeStream
              .format("memory")
              .queryName("testTxt")
              .once
              .run
          memoryDf <- SparkSession.read.table("testTxt")
          res      <- memoryDf.count
        } yield assertTrue(res == 4L)
      }
    )
}
