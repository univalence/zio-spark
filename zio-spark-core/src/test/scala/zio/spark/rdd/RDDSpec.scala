package zio.spark.rdd

import zio.spark.helper.Fixture.readRDD
import zio.spark.sql._
import zio.test._

object RDDSpec {
  def rddActionsSpec: Spec[SparkSession, Any] =
    suite("RDD Actions")(
      test("RDD should implement count correctly") {
        for {
          df     <- readRDD
          output <- df.count
        } yield assertTrue(output == 4L)
      },
      test("RDD should implement collect correctly") {
        for {
          df     <- readRDD
          output <- df.collect
        } yield assertTrue(output.length == 4)
      }
    )

  def rddTransformationsSpec: Spec[SparkSession, Any] =
    suite("RDD Transformations")(
      test("RDD should implement map correctly") {
        for {
          df <- readRDD
          transformedDf = df.map(_.age)
          output <- transformedDf.collect
        } yield assertTrue(output.headOption.contains(93))
      }
    )
}
