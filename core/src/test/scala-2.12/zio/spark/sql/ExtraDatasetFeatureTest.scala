package zio.spark.sql

import org.apache.spark.sql.Row

import zio.Task
import zio.spark.helper.Fixture._
import zio.test._
import zio.test.Assertion._

object ExtraDatasetFeatureTest {
  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  def spec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] = dataFrameActionsSpec

  def dataFrameActionsSpec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] =
    suite("ExtraDatatasetFeature Actions")(
      test("ExtraDatatasetFeature should implement summary correctly") {
        val process: DataFrame => DataFrame    = _.summary(Statistics.Count, Statistics.Max)
        val write: DataFrame => Task[Seq[Row]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(hasSize(equalTo(2))))
      },
      test("Dataset should implement explain correctly") {
        for {
          df     <- read
          transformedDf = df.withColumnRenamed("name", "fullname").filter($"age" > 30)
          _      <- transformedDf.explain
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation.contains("== Physical Plan =="))
      } @@ silent,
    )
}
