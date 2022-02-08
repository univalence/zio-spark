package zio.spark.sql

import zio.Task
import zio.spark.Fixture._
import zio.test._
import zio.test.Assertion._

object ExtraDatasetFeatureTest {
  def spec: Spec[SparkSession, TestFailure[Any], TestSuccess] = dataFrameActionsSpec

  def dataFrameActionsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("ExtraDatatasetFeature Actions")(
      test("ExtraDatatasetFeature should implement summary correctly") {
        val process: DataFrame => DataFrame    = _.summary(Statistics.Count, Statistics.Max)
        val write: DataFrame => Task[Seq[Row]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(hasSize(equalTo(2))))
      }
    )
}
