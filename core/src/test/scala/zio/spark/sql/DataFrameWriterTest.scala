package zio.spark.sql

import zio.Task
import zio.spark.helper.Fixture._
import zio.test._

object DataFrameWriterTest {
  val reader: DataFrameReader = SparkSession.read

  def dataFrameWriterSaveSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("DataFrameWriter Save")(
      test("DataFrameWriter can save a DataFrame to CSV") {
        val write: DataFrame => Task[Unit] = _.write.csv(s"$resourcesPath/output.csv")

        val pipeline = Pipeline.buildWithoutTransformation(read)(write)

        for {
          _      <- pipeline.run
          df     <- readCsv(s"$resourcesPath/output.csv")
          output <- df.count
        } yield assertTrue(output == 4L)
      }
    )
}
