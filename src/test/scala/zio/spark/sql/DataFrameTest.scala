package zio.spark.sql

import zio.test.Assertion._
import zio.test._

object DataFrameTest extends DefaultRunnableSpec {
  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite("DataFrameTest")(
      test("ZDataFrame should implement count correctly") {
        val session =
          ZSparkSession
            .builder()
            .master(Builder.LocalAllNodes)
            .appName("zio-spark")
            .getOrCreateLayer()

        val pipeline =
          Spark(
            input   = _.read.csv("src/test/resources/data.csv"),
            process = df => df,
            output  = _.count()
          )

        val job = pipeline.provideLayer(session)

        job.map(assert(_)(equalTo(4L)))
      }
    )
}
