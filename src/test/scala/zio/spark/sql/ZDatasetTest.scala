package zio.spark.sql

import zio.test.Assertion._
import zio.test._
import zio.{Task, ZLayer}

object ZDatasetTest extends DefaultRunnableSpec {
  val session: ZLayer[Any, Throwable, ZSparkSession] =
    ZSparkSession
      .builder()
      .master(Builder.LocalAllNodes)
      .appName("zio-spark")
      .getOrCreateLayer()

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    zDataFrameActionsSpec + zDataFrameTransformationsSpec

  def zDataFrameActionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("ZDataset Actions")(
      test("ZDataset should implement count correctly") {
        val read: SparkSession => Task[ZDataFrame] = _.read.inferSchema.withHeader.csv("src/test/resources/data.csv")
        val process: ZDataFrame => ZDataFrame      = df => df
        val write: ZDataFrame => Task[Long]        = _.count()

        val pipeline = Pipeline(read, process, write)

        val job = pipeline.run.provideLayer(session)

        job.map(assert(_)(equalTo(4L)))
      }
    )

  def zDataFrameTransformationsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("ZDataset Transformations")(
      test("ZDataset should implement limit correctly") {
        val read: SparkSession => Task[ZDataFrame] = _.read.inferSchema.withHeader.csv("src/test/resources/data.csv")
        val process: ZDataFrame => ZDataFrame      = _.limit(2)
        val write: ZDataFrame => Task[Long]        = _.count()

        val pipeline = Pipeline(read, process, write)

        val job = pipeline.run.provideLayer(session)

        job.map(assert(_)(equalTo(2L)))
      }
    )
}
