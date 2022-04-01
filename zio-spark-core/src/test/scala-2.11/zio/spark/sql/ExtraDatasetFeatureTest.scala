package zio.spark.sql

import zio.spark.helper.Fixture._
import zio.spark.sql.implicits._
import zio.test._
import zio.test.TestAspect._

object ExtraDatasetFeatureTest {

  def spec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] = dataFrameActionsSpec

  def dataFrameActionsSpec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] =
    suite("ExtraDatatasetFeature Actions")(
      test("Dataset should implement explain correctly") {
        for {
          df <- read
          transformedDf = df.withColumnRenamed("name", "fullname").filter($"age" > 30)
          _      <- transformedDf.explain
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation.contains("== Physical Plan =="))
      } @@ silent
    )
}
