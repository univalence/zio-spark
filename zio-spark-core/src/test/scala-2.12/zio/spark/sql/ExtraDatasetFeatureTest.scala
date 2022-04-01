package zio.spark.sql

import org.apache.spark.sql.Row
import zio.Task
import zio.spark.sql.implicits._


object ExtraDatasetFeatureTest {

  def spec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] = dataFrameActionsSpec

  def dataFrameActionsSpec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] =
    suite("ExtraDatatasetFeature Actions")(
      test("ExtraDatatasetFeature should implement summary correctly") {
        val process: DataFrame => DataFrame    = _.summary(Statistics.Count, Statistics.Max)
        val write: DataFrame => Task[Seq[Row]] = _.collect

        read.map(process).flatMap(write).map(res => assert(res)(hasSize(equalTo(2))))
      },
      test("Dataset should implement explain correctly") {
        for {
          df     <- read
          transformedDf = df.withColumnRenamed("name", "fullname").filter($"age" > 30)
          _      <- transformedDf.explain("simple")
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation.contains("== Physical Plan =="))
      } @@ silent,
    )
}
