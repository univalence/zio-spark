package zio.spark.sql

import zio.test.Assertion._
import zio.test._

object ZDataFrameTest extends DefaultRunnableSpec {
  val session =
    ZSparkSession
      .builder()
      .master(Builder.LocalAllNodes)
      .appName("zio-spark")
      .getOrCreateLayer()

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    zDataFrameActionsSpec + zDataFrameTransformationsSpec

  def zDataFrameActionsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("ZDataFrame Actions")(
      test("ZDataFrame should implement count correctly") {
        val pipeline =
          Spark(
            input   = _.read.inferSchema.withHeader.csv("src/test/resources/data.csv"),
            process = df => df,
            output  = _.count()
          )

        val job = pipeline.provideLayer(session)

        job.map(assert(_)(equalTo(4L)))
      }
    )

  def zDataFrameTransformationsSpec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("ZDataFrame Transformations")(
      test("ZDataFrame should implement limit correctly") {
        val pipeline =
          Spark(
            input   = _.read.inferSchema.withHeader.csv("src/test/resources/data.csv"),
            process = _.limit(2),
            output  = _.count()
          )

        val job = pipeline.provideLayer(session)

        job.map(assert(_)(equalTo(2L)))
      }
    )
}
